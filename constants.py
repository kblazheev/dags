egrul_web = 'https://ofdata.ru/open-data/download/egrul.json.zip'
egrul_local = '/home/kirill/egrul.json.zip'
db = 'hw3.db'
tab_telecom = '''
    CREATE TABLE IF NOT EXISTS telecom_companies(
        id integer primary key,
        name text,
        normal_name text,
        inn text,
        ogrn text,
        ogrn_date date,
        okved text
    );'''
insert_telecom = "INSERT INTO telecom_companies(name, normal_name, inn, ogrn, ogrn_date, okved) VALUES(?, ?, ?, ?, ?, ?);"
tab_vacancies = '''
            CREATE TABLE IF NOT EXISTS vacancies(
                id integer primary key,
                employer_name text,
                normal_name text,
                name text,
                description text,
                key_skills text
            );'''
insert_vacancy = "INSERT INTO vacancies(employer_name, normal_name, name, description, key_skills) VALUES(?, ?, ?, ?, ?);"
tab_skills_top = '''
            CREATE TABLE IF NOT EXISTS skills_top(
                id integer primary key,
                count text,
                name text
            );'''
insert_skill = "INSERT INTO skills_top(count, name) VALUES(?, ?);"
url = "https://api.hh.ru/vacancies"
api_url = "https://api.hh.ru/"
user_agent = {'User-agent': 'Mozilla/5.0'}
url_params = {
    "text": "middle python developer",
    "search_field": "name",
    "per_page": "100",
    "area": "113"
}