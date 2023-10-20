from DBManager import DBManager
from hh import HeadHunter


def user_interaction():
    # создаем базу данных
    db_name = 'db_headhunter'
    db_manager = DBManager()
    db_manager.create_database(db_name)

    # создаем таблицы
    db_manager.create_table_employers(db_name)
    db_manager.create_table_vacancies(db_name)

    path = "companies.json"
    hh = HeadHunter()
    # получаем список данных компаний
    employers = hh.get_employers(path)

    # получаем список данных вакансий от компаний
    vacancies = hh.get_vacancies(employers)
    # заполням таблицы данными
    db_manager.save_employers_to_db(employers, db_name)
    db_manager.save_vacancies_to_db(vacancies, db_name)
    print(f"БД {db_name} успешно создана")

    while True:
        print('Показать вакансии?\n'
              '1. да\n'
              '2. нет')
        show = input('')
        if show == '1':
            print('1. Вывести список всех компаний и количество вакансий у каждой компании.\n'
                  '2. Вывести список всех вакансий.\n'
                  '3. Показать среднюю зарплату по вакансиям.\n'
                  '4. Вывести список всех вакансий, у которых зарплата выше средней.\n'
                  '5. Поиск вакансий, по ключевому слову.')

            while True:
                how_show = input()
                if how_show == '1':
                    # Вывести список всех компаний и количество вакансий у каждой компании
                    companies_and_vacancies_count = db_manager.get_companies_and_vacancies_count(db_name)
                    for company in companies_and_vacancies_count:
                        print(company)
                    break

                elif how_show == '2':
                    # Вывести список всех вакансий
                    all_vacancies = db_manager.get_all_vacancies(db_name)
                    for vacancy in all_vacancies:
                        print(vacancy)
                    break

                elif how_show == '3':
                    # Показать среднюю зарплату по вакансиям
                    avg_salary = db_manager.get_avg_salary(db_name)
                    print(f"Средняя зарплата = {avg_salary}")
                    break

                elif how_show == '4':
                    # Вывести список всех вакансий, у которых зарплата выше средней
                    vacancies_with_higher_salary = db_manager.get_vacancies_with_higher_salary(db_name)
                    for vacancy in vacancies_with_higher_salary:
                        print(vacancy)
                    break

                elif how_show == '5':
                    # Поиск вакансий, по ключевому слову
                    keyword = input("Введите слово для поиска ")
                    vacancies_with_keyword = db_manager.get_vacancies_with_keyword(db_name, keyword)
                    if vacancies_with_keyword == []:
                        print('По данному запросу ничего не найдено.')
                    else:
                        for vacancy in vacancies_with_keyword:
                            print(vacancy)
                    break
                else:
                    print('Выберите 1, 2, 3, 4, или 5.')

        # ветка для окончания программы
        elif show == '2':
            break

