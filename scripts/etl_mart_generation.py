# 파일명: etl_mart_generation.py
import pandas as pd
import json
import re
import os
import sys
from sqlalchemy import create_engine, text
from datetime import datetime

def get_db_engine():
    # 기본 접속 DB는 소스 데이터가 있는 'bronze'로 설정
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    dbname = os.getenv("DB_NAME", "bronze") 
    
    if not all([user, password, host]):
        print("DB 연결 정보가 환경변수에 없습니다. (DB_USER, DB_PASSWORD, DB_HOST 확인 필요)")
        sys.exit(1)
        
    # [핵심] 한글 및 이모지 처리를 위해 charset=utf8mb4 필수 설정
    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4"
    return create_engine(db_url)

def main():
    print(f"[{datetime.now()}] Mart ETL 프로세스 시작")
    engine = get_db_engine()

    # Airflow 또는 환경변수에서 기간 주입 (없으면 오늘 날짜)
    # 테스트 시에는 아래 start_date, end_date를 넉넉하게(예: 2020년~2030년) 수정해서 돌려보세요.
    start_date = os.getenv("START_DATE", datetime.now().strftime('%Y-%m-%d 00:00:00'))
    end_date = os.getenv("END_DATE", datetime.now().strftime('%Y-%m-%d 23:59:59'))
    
    print(f"Target Period: {start_date} ~ {end_date}")

    # =======================================================
    # 1. 데이터 조회 (timeline_ranker)
    # =======================================================
    query_timeline = f"""
    SELECT server_id, character_id, character_name, guild_name, 
           event_code, event_date, raw_json 
    FROM timeline_ranker
    WHERE event_code IN (201, 209)
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    try:
        df = pd.read_sql(query_timeline, engine)
        print(f"-> DB에서 조회된 원본 데이터: {len(df)} 건")
    except Exception as e:
        print(f"데이터 조회 실패: {e}")
        sys.exit(1)

    if not df.empty:
        # =======================================================
        # 2. JSON 파싱 및 전처리 함수 (만능 버전)
        # =======================================================
        def extract_raid_data(row):
            try:
                raw = row['raw_json']
                
                # 데이터 없으면 Skip
                if raw is None or raw == '':
                    return pd.Series(['Skip', 'Skip', '', 0])

                data_dict = {}

                # ---------------------------------------------------
                # [JSON 파싱 로직] : dict, str, 오염된 str 모두 대응
                # ---------------------------------------------------
                if isinstance(raw, dict):
                    data_dict = raw
                elif isinstance(raw, str):
                    try:
                        # 1) 정상 JSON 시도
                        data_dict = json.loads(raw)
                    except json.JSONDecodeError:
                        try:
                            # 2) CSV 이스케이프 문자 ("") 처리
                            # 예: "{""code"": 201}" -> {"code": 201}
                            clean_raw = raw.replace('""', '"')
                            if clean_raw.startswith('"') and clean_raw.endswith('"'):
                                clean_raw = clean_raw[1:-1]
                            data_dict = json.loads(clean_raw)
                        except:
                            try:
                                # 3) 파이썬 객체 스타일 (' -> ") 처리
                                fixed_raw = raw.replace("'", "\"").replace("None", "null").replace("False", "false").replace("True", "true")
                                data_dict = json.loads(fixed_raw)
                            except:
                                # 복구 불가능하면 Skip
                                return pd.Series(['Skip', 'Skip', '', 0])
                
                # ---------------------------------------------------
                # [데이터 필터링] : 진짜 201, 209 코드인지 확인 (중요 ⭐)
                # ---------------------------------------------------
                real_code = str(data_dict.get('code', '0'))
                if real_code not in ['201', '209']:
                    # 505(아이템) 같은 게 섞여 있으면 여기서 걸러짐
                    return pd.Series(['Skip', 'Skip', '', 0])

                # ---------------------------------------------------
                # [필드 추출]
                # ---------------------------------------------------
                inner = data_dict.get('data', {})
                
                raid_name = inner.get('raidName', 'Unknown')
                mode_name = inner.get('modeName', 'Normal')
                party_name = inner.get('raidPartyName', '')
                
                # 쩔 파티 여부 판단 (Regex)
                is_sell = 0
                if party_name and re.search(r'(쩔|손님|손|잠수|수금)', party_name):
                    is_sell = 1
                
                return pd.Series([raid_name, mode_name, party_name, is_sell])

            except Exception:
                return pd.Series(['Skip', 'Skip', '', 0])

        # 함수 적용
        df[['raid_name', 'difficulty', 'party_title', 'is_sell_party']] = df.apply(extract_raid_data, axis=1)
        
        # [핵심] 'Skip'으로 처리된(아이템 획득 등) 행 삭제
        df_clean = df[df['raid_name'] != 'Skip'].copy()
        
        print(f"-> 아이템/오류 로그 제외 후 유효 데이터: {len(df_clean)} 건")

        if df_clean.empty:
            print("-> 유효한 레이드 데이터가 없습니다. (모두 필터링됨)")
            return

        # 날짜 및 시간 파생변수 생성
        df_clean['event_date'] = pd.to_datetime(df_clean['event_date'])
        df_clean['play_day_name'] = df_clean['event_date'].dt.day_name()
        df_clean['play_hour'] = df_clean['event_date'].dt.hour
        
        # 최종 적재할 컬럼만 선택
        cols = ['server_id', 'character_id', 'character_name', 'guild_name', 
                'event_date', 'play_day_name', 'play_hour', 
                'raid_name', 'difficulty', 'party_title', 'is_sell_party']
        df_mart = df_clean[cols]

        # =======================================================
        # 3. 데이터 적재 (Delete -> Insert)
        # =======================================================
        with engine.begin() as conn:
            # 해당 기간 데이터 삭제 (중복 방지)
            del_sql = text(f"DELETE FROM silver.mart_raid_log WHERE event_date BETWEEN :start AND :end")
            conn.execute(del_sql, {"start": start_date, "end": end_date})
            print("-> 기존 데이터 삭제 완료 (Clean Up)")
            
        # 데이터 Insert (이미 테이블은 존재하므로 append 사용)
        df_mart.to_sql(name='mart_raid_log', schema='silver', con=engine, if_exists='append', index=False)
        print(f"-> 'silver.mart_raid_log' 적재 성공: {len(df_mart)} 건")

    else:
        print("-> 해당 기간에 처리할 원본 데이터가 없습니다.")

if __name__ == "__main__":
    main()