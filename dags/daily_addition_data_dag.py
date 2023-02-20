from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import time
import pendulum

from scripts.db_tables import create_tables_into_db
from scripts.filling_middle_tables import filling_middle_tables
from scripts.filling_showcases import filling_showcases
from scripts.last_data_date import determ_last_date_data
from scripts.save_row_into_db import save_row_data_into_db
from scripts.lentaru_parsing import get_lentaru_news
from scripts.vedomosty_parsing import get_vedomostyru_news

last_date = determ_last_date_data()
starting_date = last_date + timedelta(days=1)


def load_lentaru_news(data_from, **context):
    get_lentaru_news(data_from, **context)


def load_vedomosty_news(data_from, **context):
    get_vedomostyru_news(data_from, **context)


# Инициализирующий даг для заполнения БД данными первоначальными данными
with DAG(dag_id="daily_addition_data_dag",
         start_date=starting_date,
         schedule_interval="0 0 * * *",
         default_args={'retries': 5,
                       'retry_delay': timedelta(minutes=5)}) as dag:
    load_vedomosty_news_task = PythonOperator(task_id="vedomosty",
                                              python_callable=load_vedomosty_news,
                                              provide_context=True,
                                              op_kwargs={
                                                  'data_from': '{{ ds }}'},
                                              dag=dag,
                                              )

    load_vedomosty_news_task.post_execute = lambda **x: time.sleep(50)

    load_lenta_news_task = PythonOperator(task_id="lenta",
                                          python_callable=load_lentaru_news,
                                          provide_context=True,
                                          op_kwargs={
                                              'data_from': '{{ ds }}'},
                                          dag=dag,
                                          )

    save_row_data_into_db_task = PythonOperator(task_id="save_row_data",
                                                python_callable=save_row_data_into_db,
                                                dag=dag,
                                                )

    filling_middle_table_task = PythonOperator(task_id="filling_middle_tables_",
                                               python_callable=filling_middle_tables,
                                               dag=dag,
                                               )

    filling_showcases_task = PythonOperator(task_id="filling_showcases_",
                                            python_callable=filling_showcases,
                                            dag=dag,
                                            )

    clearing_transit_folder_task = BashOperator(
        task_id="clearing_transit_folder",
        bash_command="cd /transit_folder && rm -rf ./*"
    )

    # Схема выполнения тасков
    (
            [load_lenta_news_task, load_vedomosty_news_task]
            >> save_row_data_into_db_task
            >> filling_middle_table_task
            >> filling_showcases_task
            >> clearing_transit_folder_task
    )
