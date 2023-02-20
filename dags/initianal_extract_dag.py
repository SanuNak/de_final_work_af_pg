from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from scripts.db_tables import create_tables_into_db
from scripts.filling_middle_tables import filling_middle_tables
from scripts.filling_showcases import filling_showcases
from scripts.save_row_into_db import save_row_data_into_db
from scripts.lentaru_parsing import get_lentaru_news
from scripts.vedomosty_parsing import get_vedomostyru_news

# Устанавливаем интервал дат для первоначальной загрузки данных за интервал дат.
data_from = '2023-02-10'
data_to = '2023-02-17'


# Переопределение функций парсинга новостных сайтов
def load_lentaru_news(data_from, data_to):
    get_lentaru_news(data_from, data_to)


def load_vedomosty_news(data_from, data_to):
    get_vedomostyru_news(data_from, data_to)


# Инициализирующий даг для заполнения БД первоначальными данными
with DAG(dag_id="initianal_extract_dag",
         start_date=datetime.now(),
         schedule=None,
         catchup=False,
         default_args={'retries': 5,
                       'retry_delay': timedelta(minutes=5)}) as dag:
    # Tasks are represented as operators

    load_vedomosty_news_task = PythonOperator(task_id="vedomosty",
                                              python_callable=load_vedomosty_news,
                                              op_kwargs={
                                                  'data_from': data_from,
                                                  'data_to': data_to},
                                              dag=dag,
                                              )

    load_lenta_news_task = PythonOperator(task_id="lenta",
                                          python_callable=load_lentaru_news,
                                          op_kwargs={
                                              'data_from': data_from,
                                              'data_to': data_to},
                                          dag=dag,
                                          )

    create_tables_task = PythonOperator(task_id="create_tables",
                                        python_callable=create_tables_into_db,
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
            >> create_tables_task
            >> save_row_data_into_db_task
            >> filling_middle_table_task
            >> filling_showcases_task
            >> clearing_transit_folder_task
    )
