from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

import json
import zipfile
import logging
import requests
from collections import Counter

default_args = {
    'owner': 'Sidorova',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def get_data_from_file():
    import pandas as pd

    logger = logging.getLogger(__name__)

    sqlite_hook = SqliteHook(sqlite_get_conn='sqlite_default')
    connection = sqlite_hook.get_conn()

    logger.info('Соединение с БД успешно!')

    path_to_file = '/home/rtstudent/egrul.json.zip'

    f = 0

    with zipfile.ZipFile(path_to_file, 'r') as zip_object:
        name_list = zip_object.namelist()
        for name in name_list:
            with zip_object.open(name) as file:
                json_data = file.read()
                data = json.loads(json_data)

                logger.info(f'Запись в БД из файла={f}')
                try:
                    df = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')

                    if {"ogrn", "inn", "kpp", "name", "data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД"}.issubset(df.columns):
                        df = df[["ogrn", "inn", "kpp", "name", "data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД"]]
                        df.columns = ["ogrn", "inn", "kpp", "name", "okved"]

                        df = df[df['okved'].str.startswith('61', na=False)]

                        df.to_sql('sid_telecom_companies', con=connection, if_exists='append', index=False)
                    else:
                        logger.info(f"{name} - Нет нужного столбца")
                        f = f + 1

                except Exception as error:
                    logger.debug(f'{error}')


def get_data_from_hh():
    logger = logging.getLogger(__name__)

    sqlite_hook = SqliteHook(sqlite_get_conn='sqlite_default')
    connection = sqlite_hook.get_conn()

    logger.info('Соединение с БД успешно!')

    try:
        url_all = 'https://api.hh.ru/vacancies?text=middle python developer&per_page=30'
        result = requests.get(url_all)

        vacancies = result.json().get('items')

        for i, vacancy in enumerate(vacancies):
            logger.info(i + 1, vacancy['name'], vacancy['url'], vacancy['alternate_url'])

            result = requests.get(vacancy['url'])

            vacancy = result.json()
            name_vacancy = vacancy['name']
            name_company = vacancy['employer']['name']
            name_descr = vacancy['description']
            key_skills = vacancy['key_skills']
            if key_skills:
                key_skills_spis = ','.join([d['name'] for d in key_skills])
                print(key_skills_spis)
            else:
                key_skills_spis = '-'

            rows = [(name_company, name_vacancy, name_descr, key_skills_spis), ]
            fields = ['company_name', 'position', 'job_description', 'key_skills']
            logger.info(f'ЗАПИСЬ ДАННЫХ  - {rows}')
            sqlite_hook.insert_rows(table='sid_vacancies', rows=rows, target_fields=fields)

    except Exception as error:
        logger.debug(f'{error}')


def count_keyskills():
    sqlite_hook = SqliteHook(sqlite_get_conn='sqlite_default')
    connection = sqlite_hook.get_conn()

    cur = connection.cursor()
    res = cur.execute("SELECT key_skills FROM sid_vacancies")
    arr = res.fetchall()

    result = []
    for n, x in enumerate(arr):
        array = []
        array = arr[n][0].replace("[", "").replace("\'", "").replace("]", "")
        array = array.split(',')
        result = result + array

    print('РЕЗУЛЬТАТ РАБОТЫ = ', sorted(dict(Counter(result)).items(), key=lambda x: x[1])[-10:])


with DAG(
        dag_id='sidorova_dag',
        default_args=default_args,
        start_date=datetime(2023, 8, 12),
        schedule_interval='@daily'
) as dag:
    create_table_for_okved = SqliteOperator(
        task_id='create_table_for_okved',
        sqlite_conn_id='sqlite_default',
        sql='''CREATE TABLE IF NOT EXISTS sid_telecom_companies (ogrn varchar, 
                                                            inn varchar,
                                                            kpp varchar,
                                                            name varchar,
                                                            okved varchar);'''
    )

    create_table_for_vacancies = SqliteOperator(
        task_id='create_table_for_vacancies',
        sqlite_conn_id='sqlite_default',
        sql='''CREATE TABLE IF NOT EXISTS sid_vacancies (company_name varchar,
                                                        position varchar,
                                                        job_description text,
                                                        key_skills varchar);'''
    )

    get_okved_data = PythonOperator(
        task_id='get_data_from_file',
        python_callable=get_data_from_file,
    )

    get_vacancy_data = PythonOperator(
        task_id='get_data_from_hh',
        python_callable=get_data_from_hh,
    )

    count_key_skills = PythonOperator(
        task_id='count_keyskills',
        python_callable=count_keyskills,
    )

    create_table_for_okved >> create_table_for_vacancies >> get_okved_data >> get_vacancy_data >> count_key_skills
