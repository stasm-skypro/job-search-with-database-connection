import psycopg2


class DBManager:
    """Класс для работы с ДБ PostgreSQL."""

    def __init__(self, connection_parameters: dict) -> None:
        """
        Инициализирует параметры подключения к базе данных.
        """
        self.__params = connection_parameters

    def get_companies_and_vacancies_count(self, data_base_name: str) -> list[tuple]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        result: list[tuple] = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT company_name, COUNT(vacancies.vacancy_id) FROM companies
                JOIN vacancies ON vacancies.company_id = companies.company_id
                GROUP BY company_name
                ORDER BY company_name
                """
            )
            result = cur.fetchall()

        conn.close()

        return result

    def get_all_vacancies(self, data_base_name: str) -> list[tuple]:
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        result: list[tuple] = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vacancy_name, companies.company_name, salary, salary_currency, vacancy_url
                FROM vacancies
                JOIN companies ON companies.company_id = vacancies.company_id
                ORDER BY vacancy_name
                """
            )
            result = cur.fetchall()

        conn.close()

        return result

    def get_avg_salary(self, data_base_name: str) -> list[tuple]:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        result: list[tuple] = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vacancy_name, salary, salary_currency FROM vacancies
                WHERE salary > (SELECT AVG(salary) FROM vacancies)
                ORDER BY salary DESC
                """
            )
            result = cur.fetchall()

        conn.close()

        return result

    def get_vacancies_with_keyword(self, data_base_name: str, keyword: str) -> list[tuple]:
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        result: list[tuple] = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vacancy_name, salary, salary_currency FROM vacancies
                WHERE %s IN (vacancy_name)
                ORDER BY vacancy_name
                """,
                (keyword,),
            )
            result = cur.fetchall()

        conn.close()

        return result


if __name__ == "__main__":
    init_connection_parameters = {"host": "localhost", "user": "postgres", "password": "1234", "port": 5433}
    dbm = DBManager(connection_parameters=init_connection_parameters)

    # Получим список всех компаний и количество вакансий у каждой компании
    result = dbm.get_companies_and_vacancies_count(data_base_name="headhunter")
    print(*list("Компания %s: %d вакансий" % (item[0], item[1]) for item in result), sep="\n")

    # Получим список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
    result = dbm.get_all_vacancies(data_base_name="headhunter")
    for item in result:
        print("Требуется %s в компанию '%s', зарплата %s %s, ссылка на вакансию: %s " % tuple(x for x in item))
    # Получим список всех вакансий, у которых зарплата выше средней по всем вакансиям
    result = dbm.get_avg_salary(data_base_name="headhunter")
    for item in result:
        print("%s зарплата - %s %s" % item)

    # Получим список всех вакансий, в названии которых содержатся переданные в метод слова
    result = dbm.get_vacancies_with_keyword(data_base_name="headhunter", keyword="аналитик")
    for item in result:
        print("%s зарплата - %s %s" % item)
