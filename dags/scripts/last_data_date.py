from datetime import datetime
import psycopg2

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """ Получаем креды для подключения к PostgreSql """

    conn = BaseHook.get_connection(conn_id)
    return conn


def determ_last_date_data():
    """ Функция для определения максимальной даты загруженных в БД данных  """

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

    # Занесение новых новостей в таблицу новостей в случае их появления в слое сырых данных (определяется по датам)
    cursor.execute("""
        SELECT MAX(publish_date) AS max_date
        FROM row_data;
    """)

    last_date = datetime.strptime(str(cursor.fetchone()[0]), "%Y-%m-%d")

    conn.commit()
    cursor.close()
    conn.close()

    return last_date
