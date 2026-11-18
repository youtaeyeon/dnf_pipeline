# 파일명: dnf_crawler_daily_dag.py
import os
import sys
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

KST = pendulum.timezone("Asia/Seoul")

# 점검 대비: 30분 간격으로 총 12번(6시간) 재시도 -> 오전 6시 실패 시 오후 12시까지 시도
default_args = {
    "owner": "dnf_bot",
    "retries": 12,                  # 재시도 횟수
    "retry_delay": pendulum.duration(minutes=30), # 재시도 간격
}

def run_crawler(**context):
    mysql = BaseHook.get_connection("mysql_conn")
    
    os.environ["DB_HOST"] = mysql.host
    os.environ["DB_PORT"] = str(mysql.port or 3306)
    os.environ["DB_USER"] = mysql.login
    os.environ["DB_PASSWORD"] = mysql.password
    os.environ["DB_NAME"] = mysql.schema or "bronze"

    script_path = "/opt/airflow/scripts"
    if script_path not in sys.path:
        sys.path.insert(0, script_path)
        
    import dundam_crawler
    import importlib
    importlib.reload(dundam_crawler)
    dundam_crawler.main()

with DAG(
    dag_id="dnf_ranker_crawler_daily",
    start_date=pendulum.datetime(2025, 1, 1, tz=KST),
    schedule="0 6 * * *", # 매일 오전 06:00
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dnf", "crawler", "daily"],
) as dag:

    t1 = PythonOperator(
        task_id="run_dundam_crawler_daily",
        python_callable=run_crawler,
    )