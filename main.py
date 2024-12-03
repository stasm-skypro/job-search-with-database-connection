from src.config import config
from src.headhunter_api import HeadHunterAPI
from src.schema_manager import SchemaManager


def main() -> None:
    """
    Код для получения данных о вакансиях из API HeadHunter, создания базы данных и взаимодействия с пользователем
    """
    # ----------- ПОЛУЧЕНИЕ ВАКАНСИЙ С САЙТА hh.ru В ФОРМАТЕ JSON -------------
    # Аргумент keyword определяет слово, по которому будет осуществлён поиск.
    # Аргумент pages определяет количество страниц, в которых будет осуществлён поиск.
    # Аргумент per_page определяет количество вакансий на странице.
    print("Введём входные данные для поиска вакансий")

    # keyword = "python"
    user_input = input("Введите ключевое слово (по умолчанию 'Python'): ").lower()
    keyword = user_input if user_input else "python"

    # pages = 10
    user_input = input("Введите количество страниц с вакансиями, которые вы хотите получить от 1 до 10 (по умолчанию 1): ").lower()
    pages = user_input if user_input else 1

    # per_page = 100
    user_input = input("Введите количество вакансий на странице от 10 до 100 (по умолчанию 10): ").lower()
    per_page = user_input if user_input else 10

    print(f"Вы ввели: ключевое слово - {keyword}, запрошено страниц - {pages}, вакансий на странице - {per_page}")
    print("Получим сырые данные из API")

    # --Создадим экземпляра класса для работы с API headhunter
    base_url: str = "https://api.hh.ru/vacancies"
    hh_api = HeadHunterAPI(url=base_url, pages=pages, per_page=per_page)
    hh_vacancies = hh_api.load_vacancies(keyword=keyword)
    print(f"Получено {len(hh_vacancies)} вакансий")

    # --Запишем результат запроса в json-файл
    from src.file_utils import JsonWorker

    json_worker = JsonWorker("data/data.json")
    json_worker.write_file(hh_vacancies)

    # -------------------- СОЗДАНИЕ БАЗЫ ДАННЫХ -------------------------------
    init_connection_parameters = (
        config()
    )  # {"host": "localhost", "user": "postgres", "password": "****", "port": 5433}

    # --Создадим базу данных
    sm = SchemaManager(connection_parameters=init_connection_parameters)
    sm.create_database(data_base_name="headhunter")

    # --Создадим таблицу companies для хранения информации о компаниях
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

    # --Создадим таблицу companies для хранения информации о вакансиях
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
    sm.insert_data(data_base_name="headhunter", vacancies_data=data)




if __name__ == "__main__":
    main()
