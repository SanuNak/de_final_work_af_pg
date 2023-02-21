
## 1. Описание проекта
	
	Проект представляет собой решение задачи по разработке механизма на основе IT технологий 
		для регулярного сбора новостей из соответствующих новостных порталов 
		с целью формирования актуальной статистики по новостям в виде витрины данных (таблицы базы данных). 
	
	Разрабатываемый механизм должен предусматреть следующие этапы: 
		- ежедневную выгрузку данных из разных новостных порталов, 
		- обработку этих первичных данных с сохранением результатов в соответствующих таблицах базы данных,
		- актуализацию статистической витрины новостных данных, с учетом ежедневных обновлений новостей.

	Первые два этапа механизма должны выполняться при условии ежедневного пополнение информации в соответствующих 
		таблицах базы данных, а последний этап должен предусматреть лишь ежедневнцю актуализацию данных, 
		так как построен на основе агрегатных операциях с использованием собранной и подготовленной информации.
		
## 2. Содержание проекта
	
	Содержание проекта предусматривает следующее. 
		
		1. Создание базы данных и соответствующих в ней таблиц.
			- БД - news
			- Таблицы:
				- Сырой слой данных, таблицы следующие:
					- row_data (Пояснение: Сохраняется информация, скаченная из порталов как есть, без обработки):
					  переменные:
						source
						publish_date
						title
						category
				- Промежуточный слой данных, таблицы следующие:
					- source_news
					  переменные:
						source_news_id
						source_news_name
					- category_news
					  переменные:
					    category_news_id
						category_news_name
					- news
					  переменные:
						title
						source_news_id
						category_news_id
						public_date
				- Слой данных для витрины, таблицы следующие:
					- category_statistics
					  переменные:
						category_statistics_id
						category_news_id
						category_news_name
						"Общеей кол-во новостей"
						"Ср кол-во новостей категрии за сут"                
						"Кол-во новостей из 'Лента'"
						"Кол-во новостей из 'Ведомости'"
						"Общеей кол-во новостей за сутки"
						"Кол-во новостей из 'Лента' за сутки"
						"Кол-во новостей из 'Ведомости' за сутки"
						"День с макс кол-ом публ по категори"
						"Понедельник"
						"Вторник"
						"Среда"
						"Четверг"
						"Пятница"
						"Суббота"
						"Воскресение"
				
		2. Реализацию выгрузки новостей из сайтов и временное сохранение данных 
			в файлах отдельно для каждого сайта в формате JSON:
				- Lenta.ru
				- Vedomosty.ru
				
		3. Реализацию разбора JSON и сохранения данных в таблицах БД 
		4. Первоначальное наполнение базы данных за необходимый интервал дат.
		5. Ежедневное пополнение таблиц базы данных новостями указанных новостных 
            порталов за прошедший день
		6. Ежедневное актуализация витрины с соответствующей статистикой с агрегацией 
            по категориям новостей. 
		7. Все процессы учитывают принципы атомарности и идемпотентности.
		8. Оркестрацию всех процессов

## 3. Примененные технологии

    Выбранная конфигурация позволяет в полной мере решить задачу проекта, 
        в преимуществах каждой технологии перечислены качества, за которые они выбраны

	В реализации проекта применены следующие технологии
		1. Система контейнирезации Docker-compose. 
			- Позволяет изолировать проект от локальной операционной системы 
                и наделить проект элементами кроссплатформенности. 
			- Также позволяет инкапсулировать внутри проекта все технологии, 
                что наделяет проект простотой настроек и запуска.
				
		2. Система оркестрации процессов - Airflow. 
			- Преимущества Airflow в 
				гибкости настроек, 
				поддержка принципов атомарности и иденпотентсности процессов,
				низкий порог знаний для использования из "коробки" основных 
                особенностей продукта, широкий набор возможностей в выстраивании потоков
				
		3. Язык программирования Python для реализации функциональности проекта, 
            таких как выгрузка данных из сайтов, работа с БД и тп. 
			- Преимущества 
				гибкость языка
				читабельность кода
				широкие возможности при реализации большинства задач
				
		4. База данных PostgreSql. 
			- Преимущества
				современность
				гибкость 
				мощность
				масштабируемость
        
        5. Библиотека Python Selenium для парсинга динамического сайта Vedomosty.ru 



## 4. Последовательность шагов при реализации проекта

	4.1. Создание структуры проекта в файловой системе.
        Структура следующая:
            de_final_work_aif_pg
                - dags
                    ..dag.py
                    - scripts
                        .py
                - logs
                - plugins
                - transit_folder
                    .JSON

	4.2. Создание и настройка файла Docker-compose для создания и запуска 
        контейнеров с технологиями, участвующими в проекте
        
        4.2.1. За основу взят Docker-compose для контейнирязации Airflow. Он скачен с 
            официального сайта Docker.

        4.2.2. Изменены следующие настройки Airflow:
            - AIRFLOW__CORE__LOAD_EXAMPLES: 'false' (отключается загрузка примеров dag)
            - AIRFLOW__CORE__ENABLE_XCOM_PICKLING: True (Включается поддержка XCOM_PICKLING)

        4.2.2. Добавление дополнительного сервис для создания бд postgres:
                image: postgres:13
                ports:
                  - 6432:5432        
                environment:
                    POSTGRES_USER: postgres_user
                    POSTGRES_PASSWORD: pass
                    POSTGRES_DB: news
                volumes:
                  - postgres-db-volume-two:/var/lib/postgresql-two/data        
                extra_hosts:
                  - "host.docker.internal:host-gateway" 
                    (доступ к контейнеру с хоста 172.17.0.1) 
        
        4.2.3. Добавление дополнительного сервис Selenium для парсинга данных с 
                сайта Vedomosty.ru. Этот сайт имеет динамическую структуру:
                  selenium:
                    image: selenium/standalone-firefox
                    ports:
                    - 4444:4444 

        4.2.4. Добавление строчки к volumes для корректного монтирования тома для 
                дополнительной БД PostgreSql.
                volumes:
                  postgres-db-volume:
                  postgres-db-volume-two:

    4.3. Создание Dockerfile для загрузки необходимых библиотек (в данном случае Selenium))
            FROM apache/airflow:2.4.3
    
            COPY requirements.txt .
            RUN pip install --upgrade pip
            RUN pip install -r requirements.txt

	4.4. Создание двух скриптов c DAG-ами:
        4.4.1 DAG для первоначальной загрузки данных за выбранный интервал дат
            - initianal_extract_dag.py
			- в параметрах DAG указываем фиксированный интервал дат
		4.4.2 DAG для ежедневного пополнения БД данными
			- daily_addition_data_dag.py
			- в параметрах DAG указываем дату параметром в виде макроса {{ ds }}
			- параметр start_date определяем скриптом python выборкой из БД 
				крайней датой всех новостей + 1 день.

	4.5. Создание скриптов обеспечивающие функциональность DAG-ов: 
        - db_tables.py - создание таблиц бд
        - lentaru_parsing.py - парсинг сайта Lenta.ru через API
        - vedomosty_parsing.py - парсинг сайта Vedomosty.ru с использованием Selenium
        - save_row_into_db.py - созранение сырых данных
        - filling_middle_tables.py - наполнение таблиц промежуточного слоя
        - filling_showcases.py - наполнения слоя витрин
        - last_data_date.py - определение даты последних загруженных новостей в бд

	4.6. Тестирование
        Ручное тестирование
	

	
