from typing import Any
import psycopg2
import psycopg2.errors


class DBManager:
    '''Класс для работы с данными БД'''
    def __init__(self):
        self.conn = psycopg2.connect(
            host='localhost',
            database="postgres",
            user='postgres',
            password='1234'
        )
        self.cursor = self.conn.cursor()
        self.conn.autocommit = True

    def connect_to_db(cls,database_name):
        conn = psycopg2.connect(
            host='localhost',
            database=database_name,
            user='postgres',
            password='1234'
        )
        cls.conn = conn

    def create_database(self, database_name):
        '''Создание базы данных'''

        try:
            self.cursor.execute(f"CREATE DATABASE {database_name}")
        except psycopg2.errors.DuplicateDatabase:
            print(f"ОШИБКА: база данных {database_name} уже существует")
        self.conn.close()

    def create_table_employers(self, db_name):
        # создание таблицы компаний
        self.conn = psycopg2.connect(
            host='localhost',
            database=db_name,
            user='postgres',
            password='1234'
        )
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute("""CREATE TABLE IF NOT EXISTS employers (
                    employer_id varchar(100),
                    name varchar(100) NOT NULL,                          
                    employer_url text,
                    CONSTRAINT pk_employers_employer_id PRIMARY KEY(employer_id)
                    )
                    """)
        except psycopg2.errors.DuplicateTable:
            print(f"Таблица с таким именем есть")
        finally:
            self.conn.close()

    def create_table_vacancies(self, db_name):
        # сщздание таблицы вакансий
        self.conn = psycopg2.connect(
            host='localhost',
            database=db_name,
            user='postgres',
            password='1234'
        )
        try:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS vacancies (
                    vacancy_id int,
                    name varchar(100) NOT NULL,
                    vacancy_url text,
                    salary int,
                    currency varchar(10),
                    currency_value real,
                    employer_id varchar(100),
                    employer_name varchar(100),

                    CONSTRAINT pk_vacancies_vacancy_id PRIMARY KEY(vacancy_id),
                    CONSTRAINT fk_vacancies_employer_id FOREIGN KEY(employer_id) REFERENCES employers(employer_id)
                    )
                    """)
        except psycopg2.errors.DuplicateTable:
            print(f"Таблица с таким именем есть")
        finally:
            self.conn.close()

    def save_employers_to_db(self, data: list[dict[str, Any]], database_name: str):
        '''Сохранение в таблицу данных о работодателях'''
        self.connect_to_db(database_name)

        with self.conn.cursor() as cursor:
            for employer in data:
                cursor.execute(
                    '''INSERT INTO employers (employer_id, name, employer_url)
                    VALUES (%s, %s, %s)''',
                    (employer['id'], employer['name'], employer['url'])
                )
        self.conn.commit()
        self.conn.close()

    def save_vacancies_to_db(self, data: list[dict[str, Any]], database_name: str):
        '''Сохранение в таблицу данных о вакансиях '''
        self.connect_to_db(database_name)
        with self.conn.cursor() as cursor:
            for vacancy in data:
                cursor.execute(
                    """INSERT INTO vacancies (vacancy_id, name, vacancy_url, salary, currency, currency_value, employer_id, employer_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (vacancy['vacancy_id'], vacancy['name'], vacancy['url'], vacancy['salary'], vacancy['currency'],
                     vacancy['currency_value'], vacancy['employer_id'], vacancy['employer_name'])
                )
        self.conn.commit()
        self.conn.close()

    def get_companies_and_vacancies_count(self, database_name):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        """
        self.connect_to_db(database_name)
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            SELECT employer_name, COUNT(*)
                            FROM vacancies
                            GROUP BY employer_name
                            ORDER BY COUNT(*) DESC
                                """)
            data = cur.fetchall()

        self.conn.close()
        return data

    def get_all_vacancies(self, database_name):
        '''
        получает список всех вакансий с указанием названия компании, названия вакансии и
        зарплаты, и ссылки на вакансию
        :param database_name:
        :return: data
        '''
        self.connect_to_db(database_name)
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            SELECT employer_name, name, salary, currency, vacancy_url
                            FROM vacancies
                                """)
            data = cur.fetchall()

        self.conn.close()
        return data

    def get_avg_salary(self, database_name):
        '''
        получает среднюю зарплату по вакансиям
        :param database_name:
        :return:
        '''
        self.connect_to_db(database_name)
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            SELECT AVG(salary*currency_value) as avg_salary_rur
                            FROM vacancies
                                """)
            data = cur.fetchall()

        self.conn.close()
        return data

    def get_vacancies_with_higher_salary(self, database_name):
        '''
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        :param database_name:
        :return:
        '''
        self.connect_to_db(database_name)
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            SELECT *
                            FROM vacancies
                            WHERE salary*currency_value > (SELECT AVG(salary*currency_value) FROM vacancies) 
                                """)
            data = cur.fetchall()

        self.conn.close()
        return data

    def get_vacancies_with_keyword(self, database_name, keyword):
        '''
        получает список всех вакансий, в названии которых содержатся переданные
        в метод слова, например python
        :param database_name:
        :param keyword:
        :return:
        '''
        self.connect_to_db(database_name)
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            SELECT * 
                            FROM vacancies
                            WHERE name LIKE '%{keyword}%'
                                """)
            data = cur.fetchall()

        self.conn.close()
        return data
