class DBManager:
    """Класс для работы с ДБ PostgreSQL."""

    def __init__(self, connection: dict) -> None:
        self.connection = connection

    def insert_data(self) -> None:
        """
        Вставляет данные в указанную таблицу.
        """

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
