import json
import os

import pytest

from src.file_utils import JsonWorker


@pytest.fixture
def json_worker(tmpdir: str) -> JsonWorker:
    """
    Фикстура экземпляра класса JsonWorker.
    @param tmpdir: Имитирует расположение json-файла.
    @return: Экземпляр класса JsonWorker.
    """
    file_name = tmpdir.join("test_data.json")
    return JsonWorker(file_name=str(file_name))


def test_write_file(json_worker: JsonWorker) -> None:
    """
    Проверяем запись в json-файл.
    @param json_worker: Экземпляр класса JsonWorker.
    @return: None
    """
    test_data = [
        {"name": "Holger Krekel", "age": 30},
        {"name": "Bruno Oliveira", "age": 25},
    ]

    json_worker.write_file(test_data)

    # Проверяем, что файл существует
    assert os.path.exists(json_worker._JsonWorker__file_name)

    # Проверяем содержимое файла
    with open(json_worker._JsonWorker__file_name, "r", encoding="utf-8") as file:
        data = json.load(file)
        assert data == test_data


def test_read_file(json_worker: JsonWorker) -> None:
    """
    Проверяем чтение из json-файла.
    @param json_worker: Экземпляр класса JsonWorker.
    @return: None
    """
    test_data = [
        {"name": "Holger Krekel", "age": 30},
        {"name": "Bruno Oliveira", "age": 25},
    ]

    # Записываем тестовые данные в файл перед чтением
    json_worker.write_file(test_data)

    # Читаем данные из файла
    data = json_worker.read_file()

    # Проверяем, что прочитанные данные совпадают с тестовыми данными
    assert data == test_data
