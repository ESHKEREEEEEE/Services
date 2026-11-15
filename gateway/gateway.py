import aio_pika
from aio_pika import connect_robust, Message, IncomingMessage
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue
from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
import json
import uuid
import asyncio
from typing import Optional, Dict

#Pydantic модель для вакансии
class Vacancy(BaseModel):
    #Название вакансии
    name: str
    #Описание вакансии
    description: str
    #Требуемый опыт
    yoe: int

#Класс RPC гейтвея
class RPCGateway:
    def __init__(self, rabbit_url: str, username: str, password: str):
        self.rabbit_url = rabbit_url
        self.username = username
        self.password = password
        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[AbstractRobustChannel] = None
        self.rpc_queue: Optional[AbstractRobustQueue] = None
        self.response_waiters: Dict[str, asyncio.Future] = {}
        self._listener_task = None

    #Подключение к RabbitMQ
    async def connect(self):

        self.connection = await connect_robust(
            self.rabbit_url,
            login=self.username,
            password=self.password
        )
        self.channel = await self.connection.channel()
        
        #Объявление очередей для отправки сообщений микросервисам
        await self.channel.declare_queue('vacancies_queue', durable=True, arguments={'x-queue-type': 'quorum'})
        await self.channel.declare_queue('cover_generator_queue', durable=True, arguments={'x-queue-type': 'quorum'})
        
        #Объявление очереди для ответов от микросервисов
        self.rpc_queue = await self.channel.declare_queue(exclusive=True)
        
        #Прослушивание ответов
        self._listener_task = asyncio.create_task(self._start_listening())
        
        print(" [GATEWAY] Connected to RabbitMQ")

    #Начать прослушивание ответов
    async def _start_listening(self):
        async with self.rpc_queue.iterator() as queue_iter:
            async for message in queue_iter:
                await self._handle_response(message)

    #Обработка ответа
    async def _handle_response(self, message: IncomingMessage):
        correlation_id = message.correlation_id
        if correlation_id and correlation_id in self.response_waiters:
            try:
                body = json.loads(message.body.decode())
                self.response_waiters[correlation_id].set_result(body)
            except Exception as e:
                self.response_waiters[correlation_id].set_exception(e)
            finally:
                if correlation_id in self.response_waiters:
                    del self.response_waiters[correlation_id]
        await message.ack()

    #Отправка RPC запроса
    async def send_rpc_request(self, routing_key: str, payload: dict, timeout: float = 5.0) -> dict:

        correlation_id = str(uuid.uuid4())
        response_future: asyncio.Future = asyncio.Future()
        
        self.response_waiters[correlation_id] = response_future
        
        message = Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            correlation_id=correlation_id,
            reply_to=self.rpc_queue.name,
            delivery_mode=2
        )
        
        await self.channel.default_exchange.publish(
            message,
            routing_key=routing_key
        )
        
        print(f" [GATEWAY] Sent to {routing_key}: {payload}")
        
        try:
            result = await asyncio.wait_for(response_future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(f"RPC request to {routing_key} timed out after {timeout}s")

    async def close(self):
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            print(" [GATEWAY] Connection closed")


gateway = RPCGateway(rabbit_url="amqp://gateway:gateway@rabbitmq/", username='gateway', password='gateway')


@asynccontextmanager
async def lifespan(app: FastAPI):
    await gateway.connect()
    yield
    await gateway.close()


app = FastAPI(lifespan=lifespan)


@app.post("/vacancies/")
async def post_vac(vac: Vacancy):
    message = {
        "type": "post",
        "name": vac.name,
        "description": vac.description,
        "yoe": vac.yoe
    }
    
    try:
        result = await gateway.send_rpc_request(
            routing_key='vacancies_queue',
            payload=message,
            timeout=5.0
        )
        return result
    except TimeoutError as e:
        return {"status": "sent", "error": str(e)}


@app.get("/vacancies/")
async def get_vac(yoe: int = 0):
    message = {
        "type": "get_yoe",
        "yoe": yoe
    }
    
    try:
        result = await gateway.send_rpc_request(
            routing_key='vacancies_queue',
            payload=message,
            timeout=5.0
        )
        return result
    except TimeoutError as e:
        return {"status": "sent", "error": str(e), "vacancies": []}

@app.get("/vacancies/{id}")
async def get_vac(id: int):
    message = {
        "type": "get_id",
        "id": id
    }
    
    try:
        result = await gateway.send_rpc_request(
            routing_key='vacancies_queue',
            payload=message,
            timeout=5.0
        )
        return result
    except TimeoutError as e:
        return {"status": "sent", "error": str(e), "vacancies": []}

@app.put("/vacancies/{id}")
async def put_vac(vac: Vacancy, id: int):
    message = {
        "type": "put",
        "name": vac.name,
        "description": vac.description,
        "yoe": vac.yoe,
        "vacancy_id": id
    }
    
    try:
        result = await gateway.send_rpc_request(
            routing_key='vacancies_queue',
            payload=message,
            timeout=5.0
        )
        return result
    except TimeoutError as e:
        return {"status": "sent", "error": str(e)}


@app.get("/cover_letter/")
async def get_cover(user_id: int, vacancy_id: int):
    message = {
        "type": "get",
        "user_id": user_id,
        "vacancy_id": vacancy_id
    }
    
    try:
        result = await gateway.send_rpc_request(
            routing_key='cover_generator_queue',
            payload=message,
            timeout=6000.0
        )
        return result
    except TimeoutError as e:
        return {"status": "sent", "error": str(e), "cover_letter": ""}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "gateway:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
