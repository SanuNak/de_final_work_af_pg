import glob
import os
import json
import psycopg2

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """ Получаем креды для подключения к PostgreSql """

    conn = BaseHook.get_connection(conn_id)
    return conn


def save_row_data_into_db():
    """ Функция для сохранения данных в таблиц с сырыми данными """
    conn_id = Variable.get("conn_id")
    pg_conn = get_conn_credentials(conn_id)
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = (
        pg_conn.host,
        pg_conn.port,
        pg_conn.login,
        pg_conn.password,
        pg_conn.schema
    )

    conn = psycopg2.connect(
        host=pg_hostname,
        port=pg_port,
        user=pg_username,
        password=pg_pass,
        database=pg_db)

    cursor = conn.cursor()

    root_dir = "/transit_folder/"
    path_preparation = os.path.join(root_dir, "**/*.json")
    file_list = glob.glob(path_preparation, recursive=True)

    # Сохраняем данные из JSON в PostgreSql в таблице сырых данных
    for json_file in file_list:
        with open(json_file, "r") as file:
            data = json.load(file)

            query_sql = """
                        INSERT INTO row_data(source, publish_date, title, category) 
                        SELECT source, publish_date, title, category 
                        FROM json_populate_recordset(NULL::row_data, %s);
                        """
            cursor.execute(query_sql, (json.dumps(data),))

    conn.commit()
    cursor.close()
    conn.close()
