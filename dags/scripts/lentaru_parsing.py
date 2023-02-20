import os
import pandas as pd
import requests as rq
import time

from datetime import datetime, timedelta

columns_for_final_table = ["source", "publish_date", "title", "category"]
file_name = "lentaru.json"


def rename_category(tbl):
    """ Стандартизация категорий новостей """

    category_name = {1: "Политика",
                     4: "Экономика",
                     5: "Наука",
                     6: "Культура",
                     7: "Спорт",
                     8: "Разное"}

    tbl.loc[
        ~tbl["category"].isin([1, 2, 3, 4, 5, 6, 7]), "category"] = category_name.get(8)
    tbl.loc[tbl["category"] == 1, "category"] = category_name.get(1)
    tbl.loc[tbl["category"] == 2, "category"] = category_name.get(1)
    tbl.loc[tbl["category"] == 3, "category"] = category_name.get(1)
    tbl.loc[tbl["category"] == 4, "category"] = category_name.get(4)
    tbl.loc[tbl["category"] == 5, "category"] = category_name.get(5)
    tbl.loc[tbl["category"] == 6, "category"] = category_name.get(6)
    tbl.loc[tbl["category"] == 7, "category"] = category_name.get(7)

    return tbl


def get_url(param_dict: dict) -> str:
    """
    Возвращает URL для запроса json таблицы со статьями

    url = 'https://lenta.ru/search/v2/process?'\
    + 'from=0&'\                       # Смещение
    + 'size=1000&'\                    # Кол-во статей
    + 'sort=2&'\                       # Сортировка по дате (2), по релевантности (1)
    + 'title_only=0&'\                 # Точная фраза в заголовке
    + 'domain=1&'\                     # ??
    + 'modified%2Cformat=yyyy-MM-dd&'\ # Формат даты
    + 'type=1&'\                       # Материалы. Все материалы (0). Новость (1)
    + 'bloc=4&'\                       # Рубрика. Экономика (4). Все рубрики (0)
    + 'modified%2Cfrom=2020-01-01&'\
    + 'modified%2Cto=2020-11-01&'\
    + 'query='                         # Поисковой запрос
    """
    hasType = int(param_dict['type']) != 0
    hasBloc = int(param_dict['bloc']) != 0

    url = 'https://lenta.ru/search/v2/process?' \
          + 'from={}&'.format(param_dict['from']) \
          + 'size={}&'.format(param_dict['size']) \
          + 'sort={}&'.format(param_dict['sort']) \
          + 'title_only={}&'.format(param_dict['title_only']) \
          + 'domain={}&'.format(param_dict['domain']) \
          + 'modified%2Cformat=yyyy-MM-dd&' \
          + 'type={}&'.format(param_dict['type']) * hasType \
          + 'bloc={}&'.format(param_dict['bloc']) * hasBloc \
          + 'modified%2Cfrom={}&'.format(param_dict['date_from']) \
          + 'modified%2Cto={}&'.format(param_dict['date_to']) \
          + 'query={}'.format(param_dict['query'])

    return url


def get_day_news(source, param_dict: dict) -> pd.DataFrame:
    """ Возвращает pd.DataFrame со списком статей
    """

    time.sleep(0.5)
    url = get_url(param_dict)
    r = rq.get(url)
    search_table = pd.DataFrame(r.json()['matches'])

    search_table["source"] = source
    search_table['pubdate'] = [datetime.fromtimestamp(x).strftime('%Y-%m-%d') for x in
                               search_table['pubdate']]

    search_table = search_table.sort_values(by='pubdate', ascending=False)

    return search_table


def save_news(source, date_from, param_copy):
    """ Сохранение новостей в формат json """

    _file_name = f"{date_from.strftime('%Y-%m-%d')}_{file_name}"
    _directori_name = f"{date_from.strftime('%Y-%m-%d')}"
    _root_path = "/transit_folder/"

    path_to_file = os.path.join(_root_path, _directori_name)

    # создаем отдельную папку для файла
    if not os.path.exists(path_to_file):
        os.makedirs(path_to_file)

    path_to_file = os.path.join(_root_path, _directori_name, _file_name)

    day_data = pd.DataFrame()
    day_data = get_day_news(source, param_copy)

    day_data = day_data[["source", "pubdate", "title", "bloc"]]
    day_data.columns = columns_for_final_table
    day_data = rename_category(day_data)
    day_data = day_data.reset_index(drop=True)

    # открываем файл для сохранения данных за день и сохраняем данные
    with open(path_to_file, 'w', newline='\n', encoding="utf-8") as jsonfile:
        day_data.to_json(jsonfile,
                         orient='records',
                         lines=False,
                         force_ascii=False)


def enumeration_of_days(source, date_from, date_to, param_copy):
    """ 'Обход' дней из периода date_from до date_to
    """

    while date_from <= date_to:
        save_news(source, date_from, param_copy)

        print(f"Загружены новости за дату - {date_from.strftime('%Y-%m-%d')}," \
              f" загружаем до даты - {date_to.strftime('%Y-%m-%d')}")

        date_from += timedelta(days=1)
        param_copy['date_from'] = date_from.strftime('%Y-%m-%d')


def get_articles(source,
                 param_dict) -> pd.DataFrame:
    """ Функция для скачивания статей интервалами """

    param_copy = param_dict.copy()
    date_from = datetime.strptime(param_copy['date_from'], '%Y-%m-%d')
    date_to = datetime.strptime(param_copy['date_to'], '%Y-%m-%d')

    if date_from > date_to:
        raise ValueError('date_from should be less than date_to')

    enumeration_of_days(source,
                        date_from,
                        date_to,
                        param_copy)


def get_lentaru_news(_date_from="2021-08-01",
                     _date_to=None,
                     **context):
    """
    Функция запуска сбора новостей за несколько дней,
    Задаем такие параметры
        query - поисковой запрос (ключевое слово)
        offset - cмещение поисковой выдачи (от 0 до size)
        size - количество статей. Ограничено время запроса, точного лимита нет. 1000 работает почти всегда
        sort - сортировка по дате: (2) - по убыванию, (3) - по возрастанию; по релевантности (1)
        title_only - точная фраза в заголовке (1)
        domain - ?
        material - материалы: Все материалы (0). Новость (1). ["0", "1", "2", "3", "4", ...]
        block - рубрика: Экономика (4). Все рубрики (0). ["0", "1", "2", "3", "4", ...]
        date_from - с даты
        date_to - по дату
    """

    source = "LentaRu"

    # обрабатываем загружаемые месяца, последний месяц может быть приравнен первому месяца
    # в таком случае загрузятся новости за один месяц
    date_from = _date_from
    if _date_to:
        date_to = _date_to
    else:
        date_to = _date_from

    # проверка на корректность дат
    if date_from > date_to:
        raise ValueError('date_from должна быть меньше чем date_to')

    # параметры для загрузки
    query = ""
    offset = 3
    size = 1000
    sort = "3"
    title_only = "0"
    domain = "1"
    material = "1"
    bloc = "0"

    param_dict = {'query': query,
                  'from': offset,
                  'size': size,
                  'date_from': date_from,
                  'date_to': date_to,
                  'sort': sort,
                  'title_only': title_only,
                  'type': material,
                  'bloc': bloc,
                  'domain': domain}

    get_articles(source=source,
                 param_dict=param_dict)


if __name__ == '__main__':
    start_time = time.time()

    get_lentaru_news()

    end_time = time.time()
    download_time = end_time - start_time
    print(f"\nС сайта 'lenta.ru' загрузка составила: {download_time / 60:.2f} мин.")
