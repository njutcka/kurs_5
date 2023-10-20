import json
import requests


class HeadHunter:
    """Класс для работы с api сайта HeadHanter"""
    def __init__(self):
        self.url = 'https://api.hh.ru/vacancies'

    def get_employers(self, path):
        # получает список компаний из файла
        with open(path, 'r', encoding="utf-8") as file:
            employers = json.load(file)
        return employers

    def get_vacancies(self, employers):
        """
        Функция для получения вакансий по id работодателя
        """
        url_vacancies = self.url
        formatted_vacancies = []
        path = "currency.json"
        with open(path) as file:
            currencies = json.load(file)

        for employer in employers:
            id = employer.get('id')
            params = {"employer_id": {id}, "areas": 113, "per_page": 100}
            response = requests.get(url_vacancies, params=params)
            if response.status_code != 200:
                raise Exception(f"Ошибка получения вакансий! Статус: {response.status_code}")
            vacancies = response.json()['items']
            for vacancy in vacancies:
                salary = vacancy['salary']['from'] if vacancy['salary'] else None
                currency = vacancy['salary']['currency'] if vacancy['salary'] else None
                formatted_vacancies.append({
                    'vacancy_id': vacancy['id'],
                    'name': vacancy['name'],
                    'url': vacancy['alternate_url'],
                    'salary': salary,
                    'currency': currency,
                    'currency_value': currencies.get(currency),
                    'employer_id': vacancy.get('employer', {}).get("id", 'N/A'),
                    'employer_name': vacancy['employer']['name']
                })
        return formatted_vacancies
