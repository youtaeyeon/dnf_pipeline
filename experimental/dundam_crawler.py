# 파일명: dundam_crawler.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import time
import random
import sys
import os

def get_db_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    dbname = os.getenv("DB_NAME", "bronze")
    
    if not all([user, password, host]):
        print("DB Env vars missing")
        sys.exit(1)
        
    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(db_url)

def main():
    print(f"[{datetime.now()}] 랭커 크롤링 시작")
    engine = get_db_engine()

    try:
        # 직업 목록 로드
        jobs_df = pd.read_sql("SELECT DISTINCT jobName FROM job_info", engine)
        job_list = jobs_df['jobName'].tolist()
    except Exception as e:
        print(f"직업 정보 로드 실패: {e}")
        sys.exit(1)

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    base_url = "https://dundam.xyz/ranking"
    
    all_data = []

    for job in job_list:
        try:
            url = f"{base_url}?search={job}" # URL 파라미터 확인 필요
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                rows = soup.select('table.ranking_table > tbody > tr')
                
                cnt = 0
                for row in rows:
                    if cnt >= 100: break
                    cols = row.find_all('td')
                    if len(cols) < 4: continue
                    
                    server = cols[1].text.strip()
                    char_name = cols[2].text.strip()
                    fame = cols[3].text.strip()
                    
                    all_data.append({
                        'collect_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'server_id': server,
                        'character_name': char_name,
                        'job_name': job,
                        'fame': fame
                    })
                    cnt += 1
                print(f"   - {job}: {cnt}명")
            else:
                print(f"   - {job}: 접속 실패 ({response.status_code})")
                
            time.sleep(random.uniform(0.5, 1.0))
            
        except Exception as e:
            print(f"   - {job} 에러: {e}")

    # 데이터 저장
    if all_data:
        df = pd.DataFrame(all_data)
        
        # 1. 이력 저장 (Append)
        df.to_sql('ranker_info', engine, if_exists='append', index=False)
        print(f"-> ranker_info: {len(df)}건 추가")
        
        # 2. 마스터 갱신 (Replace)
        df_master = df[['server_id', 'character_name', 'job_name']].drop_duplicates()
        df_master.to_sql('user_ranker', engine, if_exists='replace', index=False)
        print(f"-> user_ranker: 갱신 완료")
    else:
        print("수집 데이터 없음")

if __name__ == "__main__":
    main()