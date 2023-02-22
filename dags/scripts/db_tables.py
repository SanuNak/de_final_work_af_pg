import psycopg2

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """ Получаем креды для подключения к PostgreSql """

    conn = BaseHook.get_connection(conn_id)
    return conn


def create_tables_into_db():
    """ Функция удаления и создания новых таблиц """

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

    # Создание таблицы для сырых данных.
    # Сырой слой данных

    cursor.execute("""
        CREATE EXTENSION IF NOT EXISTS tablefunc;
        DROP TABLE IF EXISTS row_data CASCADE;
        CREATE TABLE IF NOT EXISTS row_data
            (
                row_id       INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                source       TEXT,
                publish_date DATE,
                title        TEXT, 
                category     TEXT
            );
        """)

    # Создание таблицы со справочником источников данных.
    # Промежуточный слой данных.
    cursor.execute("""
        DROP TABLE IF EXISTS source_news CASCADE;
        CREATE TABLE IF NOT EXISTS source_news
            (
                source_news_id    INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                source_news_name  TEXT,
                row_id INT,
                CONSTRAINT "FK_row_data"
                        FOREIGN KEY (row_id) REFERENCES row_data (row_id)                
            );
        """)

    # Создание таблицы со справочником наименований категорий данных.
    # Промежуточный слой данных.
    cursor.execute("""
        DROP TABLE IF EXISTS category_news CASCADE;
        CREATE TABLE IF NOT EXISTS category_news
            (
                category_news_id    INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                category_news_name  TEXT,
                row_id INT,
                CONSTRAINT "FK_row_data"
                        FOREIGN KEY (row_id) REFERENCES row_data (row_id)                
            );
        """)

    # Создание таблицы со статьями новостей.
    # Промежуточный слой данных.
    cursor.execute("""
        DROP TABLE IF EXISTS news CASCADE;
        CREATE TABLE IF NOT EXISTS news
            (
                news_id           INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                title             TEXT,
                source_news_id    INT DEFAULT (NULL),
                category_news_id  INT DEFAULT (NULL),
                public_date       DATE,
                row_id            INT,
                CONSTRAINT "FK_source_news"
                        FOREIGN KEY (source_news_id) REFERENCES source_news (source_news_id), 
                CONSTRAINT "FK_category_news"
                        FOREIGN KEY (category_news_id) REFERENCES category_news (category_news_id),
                CONSTRAINT "FK_row_data"
                        FOREIGN KEY (row_id) REFERENCES row_data (row_id)
            );
        """)

    # Создание таблицы - витрины данных.
    # Слой витрин
    cursor.execute("""
        DROP TABLE IF EXISTS category_statistics CASCADE;
        CREATE TABLE IF NOT EXISTS category_statistics
            (
                category_statistics_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                category_news_id INT,
                category_news_name TEXT,
                "Общеей кол-во новостей" INT DEFAULT (NULL),
                "Ср кол-во новостей категрии за сут" INT DEFAULT (NULL),                
                "Кол-во новостей из 'Лента'" INT DEFAULT (NULL),
                "Кол-во новостей из 'Ведомости'" INT DEFAULT (NULL),
                "Общеей кол-во новостей за сутки" INT DEFAULT (NULL),
                "Кол-во новостей из 'Лента' за сутки" INT DEFAULT (NULL),
                "Кол-во новостей из 'Ведомости' за сутки" INT DEFAULT (NULL),
                "День с макс кол-ом публ по категори" DATE,
                "Понедельник"  INT DEFAULT (NULL),
                "Вторник"  INT DEFAULT (NULL),
                "Среда"  INT DEFAULT (NULL),
                "Четверг"  INT DEFAULT (NULL),
                "Пятница"  INT DEFAULT (NULL),
                "Суббота"  INT DEFAULT (NULL),
                "Воскресение"  INT DEFAULT (NULL),
                CONSTRAINT "FK_category_news"
                        FOREIGN KEY (category_news_id) REFERENCES category_news (category_news_id)               
            );
        """)

    conn.commit()
    cursor.close()
    conn.close()
