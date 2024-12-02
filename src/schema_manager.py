import psycopg2

from psycopg2 import sql


class SchemaManager:
    """Класс для инициализации базы данных и таблиц PostgreSQL."""

    def __init__(self, connection_parameters: dict) -> None:
        """
        Инициализирует параметры подключения к базе данных.
        """
        self.__params = connection_parameters

    def create_database(self, data_base_name: str) -> None:
        """
        Создаёт базу данных для сохранения информации о компаниях и вакансиях.
        """
        conn, cur = None, None
        try:
            # Создание подключения и курсора
            conn = psycopg2.connect(dbname="postgres", **self.__params)
            conn.autocommit = True
            cur = conn.cursor()

            # Перед удалением или изменением базы данных нужно завершить все активные соединения к ней,
            # в т.ч. pgAdmin
            """Что здесь происходит:
            pg_stat_activity — это системный каталог PostgreSQL, содержащий информацию обо всех текущих соединениях.
            pg_terminate_backend(pid) завершает соединение с указанным PID.
            Фильтр pid <> pg_backend_pid() исключает текущее соединение, чтобы не отключить самих себя."""
            cur.execute(
                sql.SQL(
                    """
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s
                      AND pid <> pg_backend_pid();
                    """
                ),
                [data_base_name],
            )

            # Удаление и создание базы данных
            cur.execute("DROP DATABASE IF EXISTS %s" % data_base_name)
            cur.execute("CREATE DATABASE %s" % data_base_name)

        except psycopg2.Error as e:
            print("Ошибка при работе с базой данных!")
            print(e)

        finally:
            if "cur" in locals() and cur:
                cur.close()
            if "conn" in locals() and conn:
                conn.close()

    def create_table(self, data_base_name: str, table_name: str, query: str) -> None:
        """
        Создаёт таблицу для сохранения информации о компаниях и вакансиях.
        """
        conn = psycopg2.connect(dbname=data_base_name, **self.__params)

        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS %s" % table_name)
            cur.execute(query % table_name)

        conn.commit()
        conn.close()


if __name__ == "__main__":
    init_connection_parameters = {"host": "localhost", "user": "postgres", "password": "1234", "port": 5433}

    # Создадим базу данных
    sm = SchemaManager(connection_parameters=init_connection_parameters)
    sm.create_database(data_base_name="headhunter")

    # Создадим таблицу companies для хранения информации о компаниях
    query_to_create_companies_table = """
            CREATE TABLE %s (
                company_id VARCHAR(10) PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL ,
                company_url TEXT NOT NULL,
                company_alternate_url TEXT,
                trusted BOOL,
                CONSTRAINT unique_order UNIQUE (company_id, company_name) -- уникальность по комбинации company_id и company_name
            )
            """
    sm.create_table(data_base_name="headhunter", table_name="companies", query=query_to_create_companies_table)

    # Создадим таблицу companies для хранения информации о вакансиях
    query_to_create_vacancies_table = """
            CREATE TABLE %s (
                vacancy_id VARCHAR(10) PRIMARY KEY,
                company_id VARCHAR REFERENCES companies(company_id),
                vacancy_name VARCHAR(255) NOT NULL,
                salary_from INT,
                salary_to INT,
                salary_currency VARCHAR(3),
                published_at DATE NOT NULL,
                vacancy_url TEXT,
                requirement TEXT,
                responsibility TEXT
            )
            """
    sm.create_table(data_base_name="headhunter", table_name="vacancies", query=query_to_create_vacancies_table)
