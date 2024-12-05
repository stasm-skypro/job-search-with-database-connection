from src.config import config
from src.db_manager import DBManager
from src.headhunter_api import HeadHunterAPI
from src.schema_manager import SchemaManager


def main(arg: int) -> None:
    """
    Код для получения данных о вакансиях из API HeadHunter, создания базы данных и взаимодействия с пользователем
    """
    # ----------- ПОЛУЧЕНИЕ ВАКАНСИЙ С САЙТА hh.ru В ФОРМАТЕ JSON -------------
    # Аргумент keyword определяет слово, по которому будет осуществлён поиск.
    # Аргумент pages определяет количество страниц, в которых будет осуществлён поиск.
    # Аргумент per_page определяет количество вакансий на странице.
    print("================ Выполним запрос на сайте hh.ru ====================")
    print("Введём входные данные для поиска вакансий")
    if arg == 1:

        keyword = "python"
        pages = 1
        per_page = 10

    else:

        # keyword = "python"
        user_input = input("Введите ключевое слово (по умолчанию 'Python'): ").lower()
        keyword = user_input if user_input else "python"

        # pages = 10
        user_input = input(
            "Введите количество страниц с вакансиями, которые вы хотите получить от 1 до 10 (по умолчанию 1): "
        ).lower()
        pages = int(user_input) if user_input else 1

        # per_page = 100
        user_input = input("Введите количество вакансий на странице от 10 до 100 (по умолчанию 10): ").lower()
        per_page = int(user_input) if user_input else 10

    print(f"Вы ввели: ключевое слово - {keyword}, запрошено страниц - {pages}, вакансий на странице - {per_page}")
    print("Получим сырые данные из API")

    # --Создадим экземпляр класса для работы с API headhunter
    base_url: str = "https://api.hh.ru/vacancies"
    hh_api = HeadHunterAPI(url=base_url, pages=pages, per_page=per_page)
    hh_vacancies = hh_api.load_vacancies(keyword=keyword)
    print(f"Получено {len(hh_vacancies)} вакансий")

    # --Запишем результат запроса в json-файл
    print("Запишем результат запроса в json-файл 'data/data.json'")
    from src.file_utils import JsonWorker

    json_worker = JsonWorker("data/data.json")
    json_worker.write_file(hh_vacancies)

    # -------------------- СОЗДАНИЕ БАЗЫ ДАННЫХ -------------------------------
    print("========= Создадим базу данных 'headhunter' PostgreSQL  ============")
    init_connection_parameters = (
        config()
    )  # {"host": "localhost", "user": "postgres", "password": "****", "port": 5433}

    # --Создадим базу данных
    print("Создадим базу данных")
    sm = SchemaManager(connection_parameters=init_connection_parameters)
    sm.create_database(data_base_name="headhunter")

    # --Создадим таблицу companies для хранения информации о компаниях
    print("Создадим таблицу companies для хранения информации о компаниях")
    query_to_create_companies_table = """
                CREATE TABLE %s (
                    company_id VARCHAR(10) PRIMARY KEY,
                    company_name VARCHAR(255) NOT NULL,
                    company_url TEXT NOT NULL,
                    company_alternate_url TEXT,
                    trusted BOOL,
                    CONSTRAINT unique_order UNIQUE (company_id, company_name)
                    -- уникальность по комбинации company_id и company_name
                )
                """
    sm.create_table(data_base_name="headhunter", table_name="companies", query=query_to_create_companies_table)

    # --Создадим таблицу vacancies для хранения информации о вакансиях
    print("Создадим таблицу vacancies для хранения информации о вакансиях")
    query_to_create_vacancies_table = """
                CREATE TABLE %s (
                    vacancy_id VARCHAR(10) PRIMARY KEY,
                    company_id VARCHAR REFERENCES companies(company_id),
                    vacancy_name VARCHAR(255) NOT NULL,
                    salary INT,
                    salary_currency VARCHAR(3),
                    published_at DATE NOT NULL,
                    vacancy_url TEXT,
                    requirement TEXT,
                    responsibility TEXT
                )
                """
    sm.create_table(data_base_name="headhunter", table_name="vacancies", query=query_to_create_vacancies_table)

    # ---------------------- ЗАПОЛНЕНИЕ ТАБЛИЦ --------------------------------
    # --Прочитаем файл с данными о вакансиях в объект data
    data = json_worker.read_file()

    # --Добавим данные из объекта data в таблицы (заполним таблицы)
    print("Заполним таблицы")
    sm.insert_data(data_base_name="headhunter", vacancies_data=data)
    print("Таблицы готовы")

    # ------------ ВЫПОЛНЕНИЕ ЗАПРОСА ПОЛЬЗОВАТЕЛЯ К БАЗЕ ДАННЫХ --------------
    excepted_queries = {
        "1": "Получить список всех компаний и количество вакансий у каждой компании",
        "2": "Получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на "
        "вакансию",
        "3": "Получить среднюю зарплату по вакансиям",
        "4": "Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям",
        "5": "Получить список всех вакансий, в названии которых содержатся ключевые слова",
    }
    # --Выполним запрос к полученной базе данных.
    print("Выполним запрос к полученной базе данных")
    print("Список доступных запросов:")
    for key, value in excepted_queries.items():
        print("%s - %s" % (key, value))

    # ------------------- НАЧАЛО ПОЛЬЗОВАТЕЛЬСКОГО ЦИКЛА -----------------------
    # Пока пользователь не подтвердит завершение работы программы, выполнять выбранные запросы
    while 1:
        user_input = input("Введите номер желаемого запроса (по умолчанию - 1): ").lower()
        query_number = user_input if user_input else "1"
        # print("Вы выбрали запрос - %s" % excepted_queries[query_number])
        # print("Результат запроса:", "\n")
        dbm = DBManager(connection_parameters=init_connection_parameters)

        match query_number:

            case "1":
                # Получим список всех компаний и количество вакансий у каждой компании
                print("Вы выбрали запрос - %s" % excepted_queries[query_number])
                print("Результат запроса:", "\n")
                result = dbm.get_companies_and_vacancies_count(data_base_name="headhunter")
                print(*list("Компания %s: %d вакансий" % (item[0], item[1]) for item in result), sep="\n")
                print()

            case "2":
                # Получим список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки
                # на вакансию
                print("Вы выбрали запрос - %s" % excepted_queries[query_number])
                print("Результат запроса:", "\n")
                result = dbm.get_all_vacancies(data_base_name="headhunter")
                for item in result:
                    print(
                        "Требуется %s в компанию '%s', зарплата %s %s, ссылка на вакансию: %s "
                        % tuple(x for x in item)
                    )
                print()

            case "3":
                # Получим среднюю зарплату по вакансиям
                print("Вы выбрали запрос - %s" % excepted_queries[query_number])
                print("Результат запроса:", "\n")
                result = dbm.get_avg_salary(data_base_name="headhunter")
                print(f"Средняя зарплата по вакансиям - {round(result[0][0])} {result[0][1]}")

            case "4":
                # Получим список всех вакансий, у которых зарплата выше средней по всем вакансиям
                print("Вы выбрали запрос - %s" % excepted_queries[query_number])
                print("Результат запроса:", "\n")
                result = dbm.get_vacancies_with_higher_salary(data_base_name="headhunter")
                for item in result:
                    print("%s зарплата - %s %s" % item)
                print()

            case "5":
                # Получим список всех вакансий, в названии которых содержатся переданные в метод слова
                print("Вы выбрали запрос - %s" % excepted_queries[query_number])
                print("Результат запроса:", "\n")
                user_input = input(
                    "Введите ключевые слова для поиска через пробел (ключевое слово по умолчанию "
                    "- 'разработчик программист'): "
                ).lower()
                kw = user_input if user_input else "разработчик программист"
                result = dbm.get_vacancies_with_keyword(data_base_name="headhunter", keywords=kw)
                if not result:
                    print("Вакансий с ключевыми словами %s нет в базе" % kw)
                else:
                    for item in result:
                        print("%s зарплата - %s %s" % item)
                print()

            case _:
                # Пользователь ввёл невалидный номер запроса
                print("Вы выбрали неверный номер запроса")

        print("Можно завершить работу программы или сделать следующий запрос")
        user_input = input("Завершить работы программы: y(es) / n(o): ")
        print()
        if user_input == "y":
            break


if __name__ == "__main__":
    # Если передать 1, то программа пропустит пользовательский запрос и применит значения по умолчанию
    main()
