import psycopg2

from psycopg2 import sql


class SchemaManager:
    """Класс для инициализации базы данных и таблиц."""

    def __init__(self, connection: dict) -> None:
        self.__conn = connection

    def create_database(self, created_dbname: str) -> None:
        """
        Создаёт базу данных для сохранения информации о компаниях и вакансиях.
        """
        conn, cur = None, None
        try:
            # Создание подключения и курсора
            conn = psycopg2.connect(**self.__conn)
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
                [created_dbname],
            )

            # Удаление и создание базы данных
            cur.execute(f"DROP DATABASE IF EXISTS {created_dbname}")
            cur.execute(f"CREATE DATABASE {created_dbname}")

        except psycopg2.Error as e:
            print("Ошибка при работе с базой данных!")
            print(e)

        finally:
            if "cur" in locals() and cur:
                cur.close()
            if "conn" in locals() and conn:
                conn.close()

    def create_table(self, table_name: str) -> None:
        """
        Создаёт таблицу в соответствии с переданными параметрами.
        """
        pass


if __name__ == "__main__":
    init_conn = {"dbname": "postgres", "host": "localhost", "user": "postgres", "port": 5433}

    sm = SchemaManager(connection=init_conn)
    sm.create_database(created_dbname="headhunter")
