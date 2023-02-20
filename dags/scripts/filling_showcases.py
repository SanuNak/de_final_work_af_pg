import psycopg2

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """ Получаем креды для подключения к PostgreSql """

    conn = BaseHook.get_connection(conn_id)
    return conn


def filling_showcases():
    """
    Функция для заполнения данными таблиц слоя витрины в отношении категрий новостей
    """

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

    # Занесение данных в витрину
    cursor.execute("""
        WITH 
            all_news AS (
                    SELECT news.category_news_id,
                        cn.category_news_name,
                        COUNT(title) AS "Общеей кол-во новостей",
                        COUNT(news.news_id) / COUNT(DISTINCT news.public_date) AS "Ср кол-во новостей категрии за сутки",
                        COUNT(CASE WHEN sn.source_news_name = 'LentaRu' THEN sn.source_news_id end) AS "Кол-во новостей из 'Лента'",
                        COUNT(CASE WHEN sn.source_news_name = 'VedomostiRu' THEN sn.source_news_id end) AS "Кол-во новостей из 'Ведомости'"
                    FROM news
                        JOIN category_news cn USING (category_news_id)
                        JOIN source_news sn USING (source_news_id)
                    GROUP BY news.category_news_id, cn.category_news_name
                    ORDER BY news.category_news_id),
            yesterday_news AS (
                    SELECT news.category_news_id,
                        COUNT(title) AS "Общеей кол-во новостей за сутки",
                        COUNT(CASE WHEN sn.source_news_name = 'LentaRu' THEN sn.source_news_id end) AS "Кол-во новостей из 'Лента' за сутки",
                        COUNT(CASE WHEN sn.source_news_name = 'VedomostiRu' THEN sn.source_news_id end) AS "Кол-во новостей из 'Ведомости' за сутки"
                    FROM news
                        JOIN category_news cn USING (category_news_id)
                        JOIN source_news sn USING (source_news_id)
                    WHERE news.public_date = current_date-1
                    GROUP BY news.category_news_id
                    ORDER BY news.category_news_id),
            max_publ_categ_day AS (
                    SELECT category_news_id, 
                            public_date AS "День с макс кол-ом публ по категории"
                    FROM (SELECT category_news_id,
                            public_date, 
                            cnt_days_news, 
                            ROW_NUMBER() OVER (PARTITION BY category_news_id ORDER BY cnt_days_news DESC) AS "rn"
                    FROM (SELECT news.category_news_id,
                            news.public_date,
                            COUNT(title) AS cnt_days_news
                        FROM news
                            JOIN category_news cn USING (category_news_id)
                        GROUP BY news.category_news_id,
                            news.public_date
                        ORDER BY news.category_news_id, news.public_date, cnt_days_news) AS cnt_news) AS cnt_news_window
                    WHERE rn = 1),
            weekday_news AS (
                    SELECT news.category_news_id,
                        COUNT(CASE WHEN EXTRACT(DOW FROM news.public_date) = 1 THEN news.news_id end) AS "Понедельник",		
                        COUNT(CASE WHEN EXTRACT(DOW FROM news.public_date) = 2 THEN news.news_id end) AS "Вторник",				
                        COUNT(CASE WHEN EXTRACT(DOW FROM news.public_date) = 3 THEN news.news_id end) AS "Среда",				
                        COUNT(CASE WHEN EXTRACT(DOW FROM news.public_date) = 4 THEN news.news_id end) AS "Четверг",				
                        COUNT(CASE WHEN EXTRACT(DOW FROM news.public_date) = 5 THEN news.news_id end) AS "Пятница",				
                        COUNT(CASE WHEN EXTRACT(DOW FROM news.public_date) = 6 THEN news.news_id end) AS "Суббота",				
                        COUNT(CASE WHEN EXTRACT(DOW FROM news.public_date) = 0 THEN news.news_id end) AS "Воскресение"
                    FROM news
                        JOIN category_news cn USING (category_news_id)
                        JOIN source_news sn USING (source_news_id)
                    GROUP BY news.category_news_id
                    ORDER BY news.category_news_id)
                    
        INSERT INTO category_statistics (    
            category_news_id,
            category_news_name,
            "Общеей кол-во новостей",
            "Ср кол-во новостей категрии за сут",                
            "Кол-во новостей из 'Лента'",
            "Кол-во новостей из 'Ведомости'",
            "Общеей кол-во новостей за сутки",
            "Кол-во новостей из 'Лента' за сутки",
            "Кол-во новостей из 'Ведомости' за сутки",
            "День с макс кол-ом публ по категори",
            "Понедельник",
            "Вторник",
            "Среда",
            "Четверг",
            "Пятница",
            "Суббота",
            "Воскресение")
        SELECT          
            all_news.category_news_id,
            category_news_name,
            "Общеей кол-во новостей",
            "Ср кол-во новостей категрии за сут",                
            "Кол-во новостей из 'Лента'",
            "Кол-во новостей из 'Ведомости'",
            "Общеей кол-во новостей за сутки",
            "Кол-во новостей из 'Лента' за сутки",
            "Кол-во новостей из 'Ведомости' за сутки",
            "День с макс кол-ом публ по категори",
            "Понедельник",
            "Вторник",
            "Среда",
            "Четверг",
            "Пятница",
            "Суббота",
            "Воскресение"
        FROM all_news
        INNER JOIN yesterday_news USING(category_news_id)
        INNER JOIN max_publ_categ_day USING(category_news_id)
        INNER JOIN weekday_news USING(category_news_id);
    """)

    conn.commit()
    cursor.close()
    conn.close()
