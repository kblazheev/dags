from constants import *
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import requests
import asyncio
import time
from aiohttp import ClientSession as cs
from aiohttp import TCPConnector as tcpcn
import json
import logging
import logging.config
from pathlib import Path
import zipfile
import pandas as pd
from normalizer import normal

config = Path('/home/kirill/airflow/dags/logger.conf')
logging.config.fileConfig(fname=config, disable_existing_loggers=False)
logger = logging.getLogger('hwLogger')

default_args = {
    'owner': 'kirill',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

def telecoms(**kwargs):
    try:
        with zipfile.ZipFile(okved_local, 'r') as zip:
            with zip.open(okved_file) as file:
                okved_df = pd.read_json(file)
        logger.info(f"Данные ОКВЭД успешно загружены из архива {okved_local}")
        sh = SqliteHook(sqlite_conn_id='hw3')  
        sh.run(tab_okved)
        logger.info("Таблица okved успешно добавлена")
        data = []
        for index, row in okved_df.iterrows():
            data.append(row)
        fields = ['code', 'parent_code', 'section', 'name', 'comment']
        sh.insert_rows(
            table='okved',
            rows=data,
            target_fields=fields,
        )
        logger.info("Данные успешно добавлены в таблицу okved")
    except Exception:
        logger.critical(f"Ошибка чтения архива {okved_local}")

async def get_vacancy(url, session):
    async with session.get(url=url) as vacancy_page:
        vacancy_json = await vacancy_page.json()
        return vacancy_json
        
async def main(items, data):
    connector = tcpcn(limit=10)
    async with cs(api_url, connector=connector) as session:
        tasks = []
        for item in items:
            url = f"/{item['url'][18:]}"
            tasks.append(asyncio.create_task(get_vacancy(url, session)))
        vacancies_json = await asyncio.gather(*tasks)
    for vacancy in vacancies_json:
        description = ''
        key_skills = ''
        if (vacancy['description'] != None):
            description = vacancy['description']
        if (vacancy['key_skills'] != None):
            for skill in vacancy['key_skills']:
                key_skills += skill['name'] + ', '
            key_skills = key_skills[:-2]
        data.append([vacancy['employer']['name'], normal(vacancy['employer']['name']), vacancy['name'], description, key_skills])

def vacancies(**kwargs):
    try:
        search_result = requests.get(url, params=url_params)
        if search_result.status_code == 200:
            vacancy_list = search_result.json().get('items')
            data = []
            start = time.time()
            asyncio.run(main(vacancy_list, data))
            logger.info(f"Время выполнения, с: {time.time() - start}")
            sh = SqliteHook(sqlite_conn_id='hw3')
            sh.run(tab_vacancies)
            logger.info('Таблица vacancies успешно добавлена в БД')
            fields = ['employer_name', 'normal_name', 'name', 'description', 'key_skills']
            sh.insert_rows(
                table='vacancies',
                rows=data,
                target_fields=fields,
            )
            logger.info('Данные успешно добавлены в таблицу vacancies')
    except Exception:
        logger.critical(f"Не удалось загрузить данные поиска: {search_result.status_code}")
        logger.critical(f"Исключение: {Exception.__class__}, {Exception.args}")
        
with DAG(
    dag_id='hw3',
    default_args=default_args,
    description='DAG for home work 3',
    start_date=datetime(2023, 7, 22),
    schedule='@daily'
) as dag:
    download_egrul = BashOperator(
        task_id='download_egrul',
        bash_command=f"wget {okved_web} -O {okved_local}"
    )
    load_telecoms = PythonOperator(
        task_id='load_telecoms',
        python_callable=telecoms,
    )
    get_vacancies = PythonOperator(
        task_id='get_vacancies',
        python_callable=vacancies,
    )
    download_egrul >> load_telecoms >> get_vacancies