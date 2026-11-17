import re
import pytest
from conftest import CoverRpcClient


#Проверка успешной генерации сопроводительного письма
@pytest.mark.asyncio
async def test_generate_cover_letter_success(cover_rpc_client: CoverRpcClient, init_vacancy_db):
    # init_vacancy_db fixture is automatically invoked and provides vacancy data
    payload = {
        "user_id": 1,
        "vacancy_id": 1
    }

    correlation_id, response_correlation_id, response = (
        await cover_rpc_client.call(payload)
    )

    #Идентификатор запроса
    assert correlation_id == response_correlation_id

    #Статус ответа
    assert response["status"] == "success"

    #Идентификаторы
    assert response["user_id"] == 1
    assert response["vacancy_id"] == 1

    #Наличие сопроводительного письма
    cover_letter = response["cover_letter"]

    assert isinstance(cover_letter, str)
    assert len(cover_letter) >= 40

    #Текст на русском языке.
    assert re.search(r"[А-Яа-яЁё]", cover_letter)

    #Модель не вернула рассуждения.
    assert not re.search(
        r"<think>|thinking process:",
        cover_letter,
        flags=re.IGNORECASE
    )


#Проверка обработки запроса без обязательных полей
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"vacancy_id": 1},
        {"user_id": 1},
        {}
    ]
)
async def test_missing_required_fields(cover_rpc_client: CoverRpcClient, payload: dict):
    correlation_id, response_correlation_id, response = (
        await cover_rpc_client.call(payload)
    )

    assert correlation_id == response_correlation_id

    assert response["status"] == "error"

    assert "message" in response
    assert response["message"]


#Проверка обработки некорректного JSON
@pytest.mark.asyncio
async def test_invalid_json(cover_rpc_client: CoverRpcClient):
    invalid_body = b'{"user_id": '

    correlation_id, response_correlation_id, response = (
        await cover_rpc_client.call_raw(invalid_body)
    )

    assert correlation_id == response_correlation_id

    assert response["status"] == "error"

    assert "message" in response


#Проверка обработки запроса с несуществующим id пользователя
@pytest.mark.asyncio
async def test_non_existent_user_id(cover_rpc_client: CoverRpcClient):
    payload = {
        "user_id": 999999,
        "vacancy_id": 1
    }

    correlation_id, response_correlation_id, response = (
        await cover_rpc_client.call(payload)
    )

    assert correlation_id == response_correlation_id

    # get_user_data() возвращает хардкодные данные, поэтому статус success
    assert response["status"] == "error"
    assert "message" in response
    assert response["message"]


#Проверка обработки запроса с несуществующим id вакансии
@pytest.mark.asyncio
async def test_non_existent_vacancy_id(cover_rpc_client: CoverRpcClient):
    payload = {
        "user_id": 1,
        "vacancy_id": 999999
    }

    correlation_id, response_correlation_id, response = (
        await cover_rpc_client.call(payload)
    )

    assert correlation_id == response_correlation_id

    assert response["status"] == "error"
    assert "message" in response
    assert response["message"]