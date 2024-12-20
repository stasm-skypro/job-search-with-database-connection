from abc import ABC, abstractmethod
from typing import Any

import requests

BASE_URL = "https://api.hh.ru/vacancies"


class BaseAPI(ABC):
    """Абстрактный класс для работы с API сервиса с вакансиями"""

    @abstractmethod
    def load_vacancies(self, keyword: str) -> list[dict]:
        """Обязательный метод для получения списка вакансий.
        @param keyword: Строковая переменная, содержащая ключевое слово, по которому осуществляется первичный отбор
        вакансий.
        @return: Список вакансий.
        """
        ...


# ---------------------------------------------------------------------------------------------------------------------
class HeadHunterAPI(BaseAPI):
    """Класс для работы с API HeadHunter."""

    def __init__(self, url: str = BASE_URL, pages: int = 1, per_page: int = 10) -> None:
        """
        Инициализатор экземпляра класса.
        @param url: URL-адрес для GET-запроса (по умолчанию "https://api.hh.ru/vacancies" - все сайты группы компаний).
        @param pages: Определяет количество страниц, в которых будет осуществлён поиск (по умолчанию - 1).
        @param per_page: Количество вакансий на странице (по умолчанию - 10).
        """
        self.__url: str = url
        self.__headers: Any = {"User-Agent": "HH-User-Agent"}
        self.__params: Any = {"text": "", "page": pages, "per_page": per_page, "only_with_salary": True}
        self.__vacancies: list = []

    def __connect_to_api(self) -> requests.models.Response | None:
        """
        Метод для подключения к API и проверки статус-кода.
        @return: Ответ сервера при подключении к API.
        """
        try:
            response = requests.get(self.__url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            # Если при подключении возникла ошибка, то выводим её в консоль.
            print(e)
            return None

    def load_vacancies(self, keyword: str = "Python") -> list[dict]:
        """
        Метод для получения списка вакансий.
        @param keyword: Строковая переменная, содержащая ключевое слово, по которому осуществляется первичный отбор
        вакансий.
        @return: Список вакансий.
        """
        response = self.__connect_to_api()
        if response is None:
            return []

        self.__params["text"] = keyword
        response = requests.get(self.__url, headers=self.__headers, params=self.__params)
        vacancies = response.json()
        self.__vacancies = vacancies.get("items", [])

        return self.__vacancies


if __name__ == "__main__":
    # ----------- ПОЛУЧЕНИЕ ВАКАНСИЙ С САЙТА hh.ru В ФОРМАТЕ JSON -------------
    # Аргумент keyword определяет слово, по которому будет осуществлён поиск.
    # Аргумент pages определяет количество страниц, в которых будет осуществлён поиск.
    # Аргумент per_page определяет количество вакансий на странице.
    print("Введём входные данные для поиска вакансий")
    pages = 10
    per_page = 100
    key_word = "Python"
    print(f"Например: ключевое слово - {key_word}, запрошено страниц - {pages}, вакансий на странице - {per_page}")
    print("Получим сырые данные из API")

    # Создание экземпляра класса для работы с API сайтов с вакансиями
    hh_api = HeadHunterAPI(url=BASE_URL, pages=pages, per_page=per_page)
    hh_vacancies = hh_api.load_vacancies(keyword=key_word)
    for vacancy in hh_vacancies:
        print("Работодатель: %s, вакансия: %s" % (vacancy["employer"]["name"], vacancy["name"]))
