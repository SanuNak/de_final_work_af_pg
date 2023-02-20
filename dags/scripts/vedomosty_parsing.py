import os
import pandas as pd
import time

from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By


# Настройки для работы драйвера Selenium
options = webdriver.FirefoxOptions()
options.add_argument("--headless")
options.add_argument('--allow-profiles-outside-user-dir')
options.add_argument('--enable-profile-shortcut-manager')
options.add_argument(r'user-data-dir=./root/data')
options.add_argument('--profile-directory=Profile 1')

file_name = "vedomostiru.json"


def rename_category(tbl):
    """ Функция для стандартизации категорий новостей """

    category_name = {1: "Политика",
                     4: "Экономика",
                     5: "Наука",
                     6: "Культура",
                     7: "Спорт",
                     8: "Разное"}

    tbl.loc[~tbl["category"].isin(["politics",
                                   "business",
                                   "finance",
                                   "economics",
                                   "technology",
                                   "media",
                                   "sport"]), "category"] = category_name.get(8)
    tbl.loc[tbl["category"] == "politics", "category"] = category_name.get(1)
    tbl.loc[tbl["category"] == "business", "category"] = category_name.get(4)
    tbl.loc[tbl["category"] == "finance", "category"] = category_name.get(4)
    tbl.loc[tbl["category"] == "economics", "category"] = category_name.get(4)
    tbl.loc[tbl["category"] == "technology", "category"] = category_name.get(5)
    tbl.loc[tbl["category"] == "media", "category"] = category_name.get(6)
    tbl.loc[tbl["category"] == "sport", "category"] = category_name.get(7)

    return tbl


def get_day_news(_source, url, date_from):
    """ Возвращает pd.DataFrame со списком статей
    """

    with webdriver.Remote("http://selenium:4444/wd/hub", 
                            DesiredCapabilities.FIREFOX, 
                            options=options) as driver:

        driver.get(url)
        driver.implicitly_wait(0.5)
        articles_preview_list = driver.find_elements(By.CLASS_NAME,
                                                     "articles-preview-list")
        items = articles_preview_list[0]

        # обход статей за день
        attributs_row_list = []
        for item in items.find_elements(By.TAG_NAME, "a"):
            source = _source
            title = item.text
            publish_date = date_from.strftime('%Y-%m-%d')
            category = item.get_attribute("href").split("/")[3]

            atributs_dict = {'source': source,
                             'publish_date': publish_date,
                             'title': title,
                             'category': category}

            attributs_row_list.append(atributs_dict)

            search_table = pd.DataFrame.from_dict(attributs_row_list)

        return search_table


def save_news(source, url, date_from):
    """ Сохранение новостей в формат json """

    _file_name = f"{date_from.strftime('%Y-%m-%d')}_{file_name}"
    _directori_name = f"{date_from.strftime('%Y-%m-%d')}"
    _root_path = "/transit_folder/"

    path_to_file = os.path.join(_root_path, _directori_name)

    # создаем отдельную папку для файла
    if not os.path.exists(path_to_file):
        os.makedirs(path_to_file)
        
    path_to_file = os.path.join(_root_path, _directori_name, _file_name)

    # открываем файл для сохранения данных за день
    with open(path_to_file, 'w', newline='\n', encoding="utf-8") as jsonfile:
        day_data = get_day_news(source, url, date_from)
        day_data = rename_category(day_data)
        
        day_data.to_json(
            jsonfile,
            orient='records',
            lines=False,
            force_ascii=False)


def enumeration_of_days(source, date_from, date_to):
    """ обход дней из периода date_from до date_to
    """

    while date_from <= date_to:
        url = f"https://www.vedomosti.ru/archive/{date_from.strftime('%Y/%m/%d')}"
        save_news(source, url, date_from)

        print(f'Загружены новости за дату - {date_from.strftime("%Y-%m-%d")},' \
              f' загружаем до даты - {date_to.strftime("%Y-%m-%d")}')

        date_from += timedelta(days=1)


def get_vedomostyru_news(_date_from="2021-08-01",
                         _date_to=None,
                         **context):
    """ Функция запуска сбора новостей за несколько месяцев, т.е > 1
    """

    source = "VedomostiRu"
    time_step = timedelta(days=1)

    # обрабатываем загружаемые месяца, последний месяц может быть приравнен первому месяца
    # в таком случае загрузятся новости за один месяц
    date_from = datetime.strptime(_date_from, '%Y-%m-%d')
    if _date_to:
        date_to = datetime.strptime(_date_to, '%Y-%m-%d')
    else:
        date_to = date_from

    # проверка на корректность дат
    if date_from > date_to:
        raise ValueError('date_from должна быть меньше чем date_to')

    enumeration_of_days(source, date_from, date_to)


if __name__ == '__main__':
    start_time = time.time()

    get_vedomostyru_news()

    end_time = time.time()
    download_time = end_time - start_time
    print(f"\nС сайта 'vedomosti.ru' загрузка составила: {download_time / 60:.2f} мин.")
    
    