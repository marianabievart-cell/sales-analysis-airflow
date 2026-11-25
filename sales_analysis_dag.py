"""DAG «sales_analysis_dag»: автоматизирует все этапы проекта.

load_to_sqlite -> sql_top_products -> analyze_and_visualize -> log_success
(этап 1: загрузка, этап 2: SQL-топы, этап 3: анализ + Plotly + Excel, затем лог об успехе)
"""
import sys
from datetime import datetime
from pathlib import Path

try:                                   # Airflow 3.x
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:                    # Airflow 2.x
    from airflow import DAG
    from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parent))   # чтобы найти sales_pipeline.py рядом с DAG
import sales_pipeline as sp

with DAG(
    dag_id="sales_analysis_dag",
    description="Загрузка логов в SQLite, SQL-топы товаров, анализ в Python, графики Plotly и Excel",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",                 # ситуация меняется быстро - пересчёт раз в сутки; для ручного запуска поставьте None
    catchup=False,
    tags=["homework", "sales"],
) as dag:
    t1 = PythonOperator(task_id="load_to_sqlite", python_callable=sp.load_to_sqlite)
    t2 = PythonOperator(task_id="sql_top_products", python_callable=sp.sql_top_products)
    t3 = PythonOperator(task_id="analyze_and_visualize", python_callable=sp.analyze_and_visualize)
    t4 = PythonOperator(task_id="log_success", python_callable=sp.log_success)
    t1 >> t2 >> t3 >> t4
