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
        res: list[tuple] = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT company_name, COUNT(vacancies.vacancy_id) FROM companies
                JOIN vacancies ON vacancies.company_id = companies.company_id
                GROUP BY company_name
                ORDER BY company_name
                """
            )
            res = cur.fetchall()

        conn.close()

        return res

    def get_all_vacancies(self, data_base_name: str) -> list[tuple]:
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        res: list[tuple] = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vacancy_name, companies.company_name, salary, salary_currency, vacancy_url
                FROM vacancies
                JOIN companies ON companies.company_id = vacancies.company_id
                ORDER BY vacancy_name
                """
            )
            res = cur.fetchall()

        conn.close()

        return res

    def get_avg_salary(self, data_base_name: str) -> list[tuple]:
        """
        Получает среднюю зарплату по вакансиям
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        res: list[tuple]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT AVG(salary), salary_currency
                FROM vacancies
                WHERE salary <> 0
                GROUP BY salary_currency
                """
            )
            res = cur.fetchall()

        conn.close()

        return res


    def get_vacancies_with_higher_salary(self, data_base_name: str) -> list[tuple]:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        res: list[tuple] = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vacancy_name, salary, salary_currency FROM vacancies
                WHERE salary > (SELECT AVG(salary) FROM vacancies)
                ORDER BY salary DESC
                """
            )
            res = cur.fetchall()

        conn.close()

        return res

    def get_vacancies_with_keyword(self, data_base_name: str, keywords: str) -> list[tuple]:
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)
        res: list[tuple] = []
        with conn.cursor() as cur:
            keyword_list = keywords.split()

            try:
                # Формируем условие WHERE с использованием ILIKE (выполняет поиск независимо от регистра)
                where_clause = " OR ".join([f"vacancy_name ILIKE %s" for _ in keyword_list])

                # Полный SQL-запрос
                query = f"SELECT vacancy_name, salary, salary_currency FROM vacancies WHERE {where_clause} ORDER BY vacancy_name"

                # Выполнение параметризованного запроса
                cur.execute(query, [f"%{keyword}%" for keyword in keyword_list])

            except KeyError:
                res = []

            else:
                res = cur.fetchall()

        conn.close()

        return res


if __name__ == "__main__":
    init_connection_parameters = {"host": "localhost", "user": "postgres", "password": "1234", "port": 5433}
    dbm = DBManager(connection_parameters=init_connection_parameters)

    # Получим список всех компаний и количество вакансий у каждой компании
    print("Получим список всех компаний и количество вакансий у каждой компании")
    result = dbm.get_companies_and_vacancies_count(data_base_name="headhunter")
    print(*list("Компания %s: %d вакансий" % (item[0], item[1]) for item in result), sep="\n")
    print()

    # Получим список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
    print("Получим список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию")
    result = dbm.get_all_vacancies(data_base_name="headhunter")
    for item in result:
        print("Требуется %s в компанию '%s', зарплата %s %s, ссылка на вакансию: %s " % tuple(x for x in item))
    print()

    # Получим среднюю зарплату по вакансиям
    print("Получим среднюю зарплату по вакансиям")
    result = dbm.get_avg_salary(data_base_name="headhunter")
    print(f"Средняя зарплата по вакансиям - {round(result[0][0], 2)} {result[0][1]}")
    print()

    # Получим список всех вакансий, у которых зарплата выше средней по всем вакансиям
    print("Получим список всех вакансий, у которых зарплата выше средней по всем вакансиям")
    result = dbm.get_vacancies_with_higher_salary(data_base_name="headhunter")
    for item in result:
        print("%s зарплата - %s %s" % item)
    print()

    # Получим список всех вакансий, в названии которых содержатся переданные в метод слова
    kw = "разработчик программист"
    print("Получим список всех вакансий, в названии которых содержатся переданные в метод слова %s" % kw)
    result = dbm.get_vacancies_with_keyword(data_base_name="headhunter", keywords=kw)
    if not result:
        print("Вакансий с ключевыми словами %s нет в базе" % kw)
    else:
        for item in result:
            print("%s зарплата - %s %s" % item)
    print()
