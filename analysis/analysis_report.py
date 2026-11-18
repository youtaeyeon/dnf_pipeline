import matplotlib
# [핵심 수정] 그래픽 창을 띄우지 않고 파일로 저장하는 모드('Agg')로 강제 설정
matplotlib.use('Agg') 

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os
import sys

# 1. DB 연결
def get_db_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    dbname = os.getenv("DB_NAME", "bronze")
    
    if not all([user, password, host]):
        print("환경변수 에러: DB 접속 정보를 확인하세요.")
        sys.exit(1)
        
    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4"
    return create_engine(db_url)

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic' 
plt.rcParams['axes.unicode_minus'] = False

def main():
    engine = get_db_engine()
    
    print(">>> 데이터 조회 중 (silver.mart_raid_log)...")
    df = pd.read_sql("SELECT * FROM silver.mart_raid_log", engine)
    
    if df.empty:
        print("데이터가 없습니다.")
        return

    print(f"총 데이터: {len(df)}건 Load 완료\n")

    # ==============================================================================
    # 분석 1. [상관관계 분석] 시간대별 쩔 파티 비율
    # ==============================================================================
    print("--- [분석 1] 시간대별 쩔 파티 비율 분석 ---")

    hourly_stats = df.groupby('play_hour')['is_sell_party'].mean() * 100
    hourly_count = df.groupby('play_hour')['log_id'].count()
    
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # 막대 그래프
    sns.barplot(x=hourly_count.index, y=hourly_count.values, color='lightgray', alpha=0.5, ax=ax1, label='전체 파티 수')
    ax1.set_ylabel('전체 파티 수 (건)', color='gray')
    
    # 선 그래프
    ax2 = ax1.twinx()
    sns.lineplot(x=hourly_stats.index, y=hourly_stats.values, color='red', marker='o', linewidth=2, ax=ax2, label='쩔 파티 비율(%)')
    ax2.set_ylabel('쩔 파티 비율 (%)', color='red')
    ax2.set_ylim(0, max(hourly_stats.values) * 1.2)

    plt.title("시간대별 전체 파티 중 쩔 파티 비율")
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    
    # [수정] plt.show() 대신 파일 저장
    filename1 = 'analysis_correlation.png'
    plt.savefig(filename1)
    print(f"✅ 그래프 저장 완료: {filename1}")

    # ==============================================================================
    # 분석 2. [분포 분석] 요일별 트래픽 분포
    # ==============================================================================
    print("\n--- [분석 2] 요일별 레이드 트래픽 분포 ---")
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daily_count = df['play_day_name'].value_counts().reindex(day_order)
    
    plt.figure(figsize=(10, 6))
    colors = ['gray' if day != 'Thursday' else 'red' for day in day_order]
    
    sns.barplot(x=daily_count.index, y=daily_count.values, palette=colors)
    plt.title("요일별 레이드 트래픽 분포 (목요일 강조)")
    plt.ylabel("레이드 생성 수")
    plt.xlabel("요일")
    
    # 비율 계산 텍스트 출력
    total_traffic = daily_count.sum()
    thu_traffic = daily_count['Thursday']
    thu_ratio = (thu_traffic / total_traffic) * 100
    
    if thu_ratio > 20:
        plt.text(3, thu_traffic, f"{thu_ratio:.1f}% (집중)", ha='center', va='bottom', color='red', fontweight='bold')
    
    # [수정] plt.show() 대신 파일 저장
    filename2 = 'analysis_distribution.png'
    plt.savefig(filename2)
    print(f"✅ 그래프 저장 완료: {filename2}")

    print("\n[완료] 실행된 폴더에 이미지 파일 2개가 생성되었습니다!")

if __name__ == "__main__":
    main()