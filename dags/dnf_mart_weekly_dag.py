# 파일명: dnf_mart_weekly_dag.py
import os
import sys
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "dnf_analyst",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

def run_mart_etl(**context):
    # 1. DB Connection Hook
    mysql = BaseHook.get_connection("mysql_conn")
    
    os.environ["DB_HOST"] = mysql.host
    os.environ["DB_PORT"] = str(mysql.port or 3306)
    os.environ["DB_USER"] = mysql.login
    os.environ["DB_PASSWORD"] = mysql.password
    os.environ["DB_NAME"] = mysql.schema or "bronze"

    # 2. 날짜 범위 설정 (지난주 목요일 ~ 이번주 목요일)
    # data_interval_start/end는 스케줄 주기에 따라 자동 할당됨
    start_kst = context["data_interval_start"].in_timezone(KST)
    end_kst = context["data_interval_end"].in_timezone(KST).subtract(seconds=1)
    
    os.environ["START_DATE"] = start_kst.strftime("%Y-%m-%d %H:%M:%S")
    os.environ["END_DATE"] = end_kst.strftime("%Y-%m-%d %H:%M:%S")

    # 3. 스크립트 실행
    script_path = "/opt/airflow/scripts"
    if script_path not in sys.path:
        sys.path.insert(0, script_path)
        
    import etl_mart_generation
    import importlib
    importlib.reload(etl_mart_generation) # 모듈 캐싱 방지
    etl_mart_generation.main()

with DAG(
    dag_id="dnf_mart_weekly_update",
    start_date=pendulum.datetime(2025, 1, 1, tz=KST),
    schedule="0 11 * * 4", # 매주 목요일 11:00
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dnf", "mart", "weekly"],
) as dag:

    t1 = PythonOperator(
        task_id="run_etl_mart_generation",
        python_callable=run_mart_etl,
    )