import pika
from pika.credentials import PlainCredentials
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

class Gateway:
    def __init__(self, rabbit_port: int, username: str, password: str):
        self.rabbit_port = rabbit_port
        self.username = username
        self.password = password 
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = PlainCredentials(self.username, self.password, erase_on_connect=True)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', port=self.rabbit_port, credentials=credentials)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='vacancies_queue', durable=True, arguments={'x-queue-type': 'quorum'})
        self.channel.queue_declare(queue='cover_generator_queue', durable=True, arguments={'x-queue-type': 'quorum'})
        print(" [GATEWAY] Connected to RabbitMQ")

    def send_message(self, routing_key: str, body: str):
        if not self.channel:
            raise RuntimeError("RabbitMQ connection is not established.")

        if self.channel.is_closed:
            raise RuntimeError("RabbitMQ channel is closed.")
        
        self.channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=body
        )
        print(f" [GATEWAY] Sent to {routing_key}: {body}")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print(" [GATEWAY] Connection closed")

gateway = Gateway(rabbit_port=5672, username='gateway', password='gateway')

@asynccontextmanager
async def lifespan(app: FastAPI):
    gateway.connect()
    yield
    gateway.close()


app = FastAPI(lifespan=lifespan)

@app.post("/vacancies/")
def post_vac():
    gateway.send_message(routing_key='vacancies_queue', body='past_vac_placeholder')
    return {"status": "sent"}

@app.get("/vacancies/")
def get_vac(yoe: int = 0):
    gateway.send_message(routing_key='vacancies_queue', body=f'get_vac_placeholder_yoe_{yoe}')
    return {"status": "sent"}

@app.put("/vacancies/{id}")
def put_vac(id: int):
    gateway.send_message(routing_key='vacancies_queue', body=f'put_vac_placeholder_id_{id}')
    return {"status": "sent"}

@app.get("/cover_letter/")
def get_cover(user_id: int, vacancy_id: int):
    gateway.send_message(routing_key='cover_generator_queue', body=f'user_{user_id}_vac_{vacancy_id}')
    return {"status": "sent"}

if __name__ == "__main__":
    uvicorn.run(
        "gateway:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )