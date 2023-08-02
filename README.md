# Домашняя работа 3: python для ETL (Блажеев Кирилл)

## Установка
Для Windows 10 устанавливаем WSL
Из под WSL (Ubuntu) устанавливаем:
     - python 3.10.11
     - apache airflow
     - необходимые модули python из файла requirements.txt (python -m pip install -r .\requirements.txt)

## Конфигурация и настройка
constants.py - настройка приложения
logger.conf - настройка логирования
normalizer.py - настройка словаря нормализации / транслитерации

## Запуск
Apache airflow запускается из под WSL (Ubuntu) командой airflow standalone
Запуск приложения производится из web-интерфейса apache airflow (localhost:8080). Dag_id - "hw3"
Последовательно выполняются таски:
    download_egrul >> load_telecoms >> get_vacancies >> top_skills
    - download_egrul - загрузка файла ЕГРЮЛ
    - load_telecoms - выбор из файла ЕГРЮЛ компаний в сфере телеком, загрузка в БД hw3.db
    - get_vacancies - получение списка 100 вакансий middle python developer через API интерфейс сайта hh.ru, загрузка в БД hw3.db
    - top_skills - отбор из полученных вакансий топ 10 ключевых навыков для компаний в сфере телеком, выгрузка в файл skills.xlsx

