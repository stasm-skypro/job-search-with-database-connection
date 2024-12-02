import psycopg2


class DBManager:
    """Класс для работы с ДБ PostgreSQL."""

    def __init__(self, connection_parameters: dict) -> None:
        """
        Инициализирует параметры подключения к базе данных.
        """
        self.__params = connection_parameters

    def insert_data(self, data_base_name: str, vacancies_data) -> None:
        """
        Сохраняет данные о компаниях и вакансиях в указанную таблицу.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)

        with conn.cursor() as cur:
            cur.execute("TRUNCATE companies RESTART IDENTITY CASCADE")

            already_inserted = []
            for vacancy in vacancies_data:
                if (vacancy["employer"]["id"], vacancy["employer"]["name"]) not in already_inserted:
                    cur.execute(
                        """
                        INSERT INTO companies (company_id, company_name, company_url, company_alternate_url, trusted)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            vacancy["employer"]["id"],
                            vacancy["employer"]["name"],
                            vacancy["employer"]["url"],
                            vacancy["employer"]["alternate_url"],
                            vacancy["employer"]["trusted"],
                        ),
                    )
                    already_inserted.append((vacancy["employer"]["id"], vacancy["employer"]["name"]))

                if vacancy["salary"] is None:
                    salary_from = 0
                    salary_to = 0
                    salary_currency = None
                else:
                    salary_from = vacancy["salary"]["from"]
                    salary_to = vacancy["salary"]["to"]
                    salary_currency = vacancy["salary"]["currency"]

                    if vacancy["salary"]["from"] is None:
                        salary_from = 0

                    if vacancy["salary"]["to"] is None:
                        salary_to = 0

                if vacancy["snippet"] is None:
                    requirement = None
                    responsibility = None
                else:
                    requirement = vacancy["snippet"]["requirement"]
                    responsibility = vacancy["snippet"]["responsibility"]

                cur.execute(
                    """
                    INSERT INTO vacancies (vacancy_id, company_id, vacancy_name, salary_from, salary_to,
                    salary_currency, published_at, vacancy_url, requirement, responsibility)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        vacancy["id"],
                        vacancy["employer"]["id"],
                        vacancy["name"],
                        salary_from,
                        salary_to,
                        salary_currency,
                        vacancy["published_at"],
                        vacancy["url"],
                        requirement,
                        responsibility,
                    ),
                )

        conn.commit()
        conn.close()

    def get_companies_and_vacancies_count(self) -> list[dict]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        """

    def get_all_vacancies(self) -> list[dict]:
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию.
        """

    def get_avg_salary(self) -> float:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """

    def get_vacancies_with_keyword(self) -> list[dict]:
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.
        """


if __name__ == "__main__":
    from src.file_utils import JsonWorker

    # Прочитаем файл с данными о вакансиях в объект data
    json_worker = JsonWorker()
    data = json_worker.read_file()

    # Добавим данные из объекта data в таблицы
    init_connection_parameters = {"host": "localhost", "user": "postgres", "password": "1234", "port": 5433}
    dbm = DBManager(connection_parameters=init_connection_parameters)

    # Добавим данные из объекта data в таблицу companies
    dbm.insert_data(data_base_name="headhunter", vacancies_data=data)
