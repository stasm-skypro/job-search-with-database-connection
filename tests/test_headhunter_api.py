from unittest.mock import MagicMock, patch

import pytest

from src.headhunter_api import HeadHunterAPI


@pytest.fixture
def hh_api() -> HeadHunterAPI:
    """
    Заглушка для API.
    @return: экземпляр класса HeadHunterAPI.
    """
    return HeadHunterAPI()


@patch("src.headhunter_api.requests.get")
def test_connect_to_api(mock_get: MagicMock, hh_api: MagicMock) -> None:
    """
    Проверяем подключение к API.
    @param mock_get: Заглушка для метода requests.get.
    @param hh_api: Заглушка для экземпляра класса HeadHunterAPI.
    @return:
    """
    mock_response = mock_get.return_value  # Заглушка для имитации валидного ответа.
    mock_response.raise_for_status.return_value = None  # Заглушка для имитации ответа None из метода __connect_to_api.

    response = hh_api._HeadHunterAPI__connect_to_api()
    assert response is not None
    mock_get.assert_called_once_with(hh_api._HeadHunterAPI__url)


@patch("src.headhunter_api.requests.get")
def test_load_vacancies(mock_get: MagicMock, hh_api: MagicMock) -> None:
    """
    Проверяем получение данных из API.
    @param mock_get: Заглушка для метода requests.get.
    @param hh_api: Заглушка для экземпляра класса HeadHunterAPI.
    @return: None
    """
    mock_response = mock_get.return_value
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"items": [{"id": 1, "name": "Охраняющий"}]}

    vacancies = hh_api.load_vacancies("Python")
    assert len(vacancies) == 1
    assert vacancies[0]["id"] == 1
    assert vacancies[0]["name"] == "Охраняющий"
    mock_get.assert_called_with(
        hh_api._HeadHunterAPI__url,
        headers=hh_api._HeadHunterAPI__headers,
        params=hh_api._HeadHunterAPI__params,
    )
