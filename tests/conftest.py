import aio_pika
import asyncio
import json
import uuid
import pytest_asyncio
from typing import Any


class CoverRpcClient:

    def __init__(self, channel, reply_queue, routing_key: str = "cover_generator_queue"):
        self.channel = channel
        self.reply_queue = reply_queue
        self.routing_key = routing_key

        #Список запросов, для которых ожидаются ответы
        self.pending: dict[str, asyncio.Future] = {}

    async def start(self):
        #Подписка на очередь ответов
        self.consumer_tag = await self.reply_queue.consume(
            self.on_response,
            no_ack=True
        )

    async def on_response(self, message: aio_pika.IncomingMessage):
        correlation_id = message.correlation_id

        #Future конкретного запроса
        future = self.pending.get(correlation_id)

        if future is None or future.done():
            return

        try:
            response = json.loads(message.body.decode("utf-8"))

            future.set_result(
                (correlation_id, response)
            )

        except Exception as e:
            future.set_exception(e)

    async def call_raw(self, body: bytes, timeout: int = 120):
        correlation_id = str(uuid.uuid4())

        #Создание Future
        future = asyncio.get_running_loop().create_future()

        self.pending[correlation_id] = future

        try:
            #Отправка сообщения
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=body,
                    reply_to=self.reply_queue.name,
                    correlation_id=correlation_id,
                    content_type="application/json"
                ),
                routing_key=self.routing_key
            )

            #Ожидание ответа
            response_correlation_id, response = await asyncio.wait_for(
                future,
                timeout=timeout
            )

            return correlation_id, response_correlation_id, response

        finally:
            self.pending.pop(correlation_id, None)

    async def call(self, payload: dict[str, Any], timeout: int = 120):
        body = json.dumps(
            payload,
            ensure_ascii=False
        ).encode("utf-8")

        return await self.call_raw(body, timeout)


@pytest_asyncio.fixture
async def vacancy_rpc_client():
    connection = await aio_pika.connect_robust("amqp://vacancies:vacancies@rabbitmq/", login='tests', password='tests')

    try:
        channel = await connection.channel()

        #Объявление очереди вакансий
        await channel.declare_queue(
            "vacancies_queue",
            passive=True,
            durable=True,
            arguments={"x-queue-type": "quorum"}
        )

        #Объявление очереди для ответов
        reply_queue = await channel.declare_queue(
            "",
            exclusive=True,
            auto_delete=True
        )

        client = CoverRpcClient(channel, reply_queue, routing_key="vacancies_queue")

        await client.start()

        yield client

    finally:
        await connection.close()


@pytest_asyncio.fixture
async def cover_rpc_client():
    connection = await aio_pika.connect_robust("amqp://vacancies:vacancies@rabbitmq/", login='tests', password='tests')

    try:
        channel = await connection.channel()

        #Объявление очереди генератора
        await channel.declare_queue(
            "cover_generator_queue",
            passive=True,
            durable=True,
            arguments={"x-queue-type": "quorum"}
        )

        #Объявление очереди для ответов
        reply_queue = await channel.declare_queue(
            "",
            exclusive=True,
            auto_delete=True
        )

        client = CoverRpcClient(channel, reply_queue, routing_key="cover_generator_queue")

        await client.start()

        yield client

    finally:
        await connection.close()


@pytest_asyncio.fixture
async def init_vacancy_db():
    connection = await aio_pika.connect_robust(
        "amqp://vacancies:vacancies@rabbitmq/",
        login="tests",
        password="tests"
    )

    try:
        channel = await connection.channel()

        await channel.declare_queue(
            "vacancies_queue",
            passive=True,
            durable=True,
            arguments={"x-queue-type": "quorum"}
        )

        reply_queue = await channel.declare_queue(
            "",
            exclusive=True,
            auto_delete=True
        )

        client = CoverRpcClient(channel, reply_queue, routing_key="vacancies_queue")
        await client.start()

        payload = {
            "type": "post",
            "name": "Senior Backend Developer",
            "description": "Разработка серверных приложений на Python",
            "yoe": 5
        }

        #Отправляем запрос и ожидаем ответ
        correlation_id, response_correlation_id, response = (
            await client.call(payload, timeout=30)
        )

        assert correlation_id == response_correlation_id

        #Вакансия действительно создана
        assert response["status"] == "success", response

        yield response

    finally:
        await connection.close()