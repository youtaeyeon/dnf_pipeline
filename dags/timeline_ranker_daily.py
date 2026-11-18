import os
import sys
import pendulum

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "retries": 12,                               # 점검 대비
    "retry_delay": pendulum.duration(minutes=30),
}

def run_ranker_timeline_loader(**context):
    # -----------------------------
    # 1) DB Connection (mysql_conn)
    # -----------------------------
    mysql = BaseHook.get_connection("mysql_conn")

    os.environ["DB_HOST"] = mysql.host
    os.environ["DB_PORT"] = str(mysql.port or 3306)
    os.environ["DB_USER"] = mysql.login
    os.environ["DB_PASSWORD"] = mysql.password
    os.environ["DB_NAME"] = mysql.schema or "bronze"

    # -----------------------------
    # 2) 날짜 계산 (요청한 로직)
    # -----------------------------
    # 실행 시각: 06:00
    # 수집 기간:
    #   start = 전일 06:00:00
    #   end   = 금일 05:59:59
    start_kst = context["data_interval_start"].in_timezone(KST)
    end_kst = context["data_interval_end"].in_timezone(KST).subtract(seconds=1)

    os.environ["START_DATE"] = start_kst.strftime("%Y-%m-%d %H:%M:%S")
    os.environ["END_DATE"] = end_kst.strftime("%Y-%m-%d %H:%M:%S")

    # -----------------------------
    # 3) 실행 옵션
    # -----------------------------
    os.environ["CHUNK_SIZE"] = "100"
    os.environ["TOP_N"] = "0"
    os.environ["TABLE_NAME"] = "timeline_ranker"
    os.environ["RANKER_TABLE"] = "ranker_info"

    # API Key (Variable)
    os.environ["NEOPLE_API_KEY"] = Variable.get("NEOPLE_API_KEY")

    # -----------------------------
    # 4) 수집 스크립트 실행
    # -----------------------------
    if "/opt/airflow/scripts" not in sys.path:
        sys.path.insert(0, "/opt/airflow/scripts")

    import ranker_timeline_loader
    ranker_timeline_loader.main()

with DAG(
    dag_id="timeline_ranker_daily",
    start_date=pendulum.datetime(2025, 1, 1, tz=KST),
    schedule="0 6 * * *",        # ✅ 매일 06:00:00
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dnf", "neople", "timeline"],
) as dag:

    run_loader = PythonOperator(
        task_id="run_ranker_timeline_loader",
        python_callable=run_ranker_timeline_loader,
    )
