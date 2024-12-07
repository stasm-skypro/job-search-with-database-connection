import json
import os
from typing import Any


class JsonWorker:
    """Класс для работы json-файлами."""

    def __init__(self, file_name: str = "data/data.json") -> None:
        """
        Инициализатор экземпляра класса.
        @param file_name: Строковая переменная, содержащая относительный путь к файлу.
        """
        self.__file_name = file_name

    def write_file(self, data: list) -> None:
        """
        Записывает json-объект в json-файл.
        @param data: JSON-объект (список словарей), предназначенный для записи в файл.
        @return: None
        """
        full_path = os.path.abspath(self.__file_name)
        with open(full_path, "w", encoding="UTF-8") as file:
            json.dump(data, file, ensure_ascii=False)

    def read_file(self) -> list[dict] | Any:
        """
        Читает содержимое json-файла в json-объект (список словарей).
        @return: JSON-объект (список словарей).
        """
        full_path = os.path.abspath(self.__file_name)
        with open(full_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        return data


if __name__ == "__main__":
    from src.headhunter_api import HeadHunterAPI

    # Получим данные о вакансиях из Head Hunter API
    BASE_URL = "https://api.hh.ru/vacancies"
    pages = 10
    per_page = 100
    keyword = "Python"

    hh_api = HeadHunterAPI(url=BASE_URL, pages=pages, per_page=per_page)
    hh_vacancies = hh_api.load_vacancies(keyword=keyword)

    # Запишем их в файл (по умолчанию /data/data.json)
    json_worker = JsonWorker()
    json_worker.write_file(hh_vacancies)
