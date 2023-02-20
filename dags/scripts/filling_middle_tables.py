import psycopg2

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """ Получаем креды для подключения к PostgreSql """

    conn = BaseHook.get_connection(conn_id)
    return conn


def filling_middle_tables():
    """ Функция для заполнения данными таблиц среднего слоя промежуточным данными """

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

    # Занесение новых источников в справочник источников в случае их появления в слое сырых данных
    cursor.execute("""
        INSERT INTO source_news (source_news_name)
        SELECT DISTINCT source
        FROM row_data
        LEFT JOIN source_news ON row_data.source = source_news.source_news_name
        WHERE source_news.source_news_name IS NULL;
    """)

    # Занесение новых категорий в справочник категорий в случае их появления в слое сырых данных
    cursor.execute("""
        INSERT INTO category_news (category_news_name)
        SELECT DISTINCT category
        FROM row_data
        LEFT JOIN category_news ON row_data.category = category_news.category_news_name
        WHERE category_news.category_news_name IS NULL;
    """)

    # Занесение новых новостей в таблицу новостей в случае их появления в слое сырых данных (определяется по датам)      
    cursor.execute("""
        INSERT INTO news (source_news_id, category_news_id, title, public_date)   
        SELECT source_news.source_news_id, category_news.category_news_id, row_data.title, row_data.publish_date
        FROM row_data
        LEFT JOIN source_news ON row_data.source = source_news.source_news_name
        LEFT JOIN category_news ON row_data.category = category_news.category_news_name
        LEFT JOIN news ON row_data.publish_date = news.public_date
        WHERE news.public_date IS NULL;
    """)

    conn.commit()
    cursor.close()
    conn.close()
