import aio_pika
from aio_pika import connect_robust, Message
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue
import asyncio
import json
import sys
import os


#Обработка запросов
async def process_vacancy_request(message: aio_pika.IncomingMessage, channel: AbstractRobustChannel) -> dict:
    try:
        body = json.loads(message.body.decode())
        msg_type = body.get("type", "unknown")

        #Обработка POST запроса
        if msg_type == "post":
            #Болванка
            response = {
                "status": "created",
                "vacancy": {
                    "id": 1,
                    "name": body.get("name", "Unknown"),
                    "description": body.get("description", ""),
                    "yoe": body.get("yoe", 0)
                }
            }

        #Обработка GET запроса
        elif msg_type == "get":
            #Болванка
            yoe = body.get("yoe", 0)
            response = {
                "status": "success",
                "vacancies": [
                    {"id": 1, "name": "Software Engineer", "description": "Backend development", "yoe": 3},
                    {"id": 2, "name": "Data Analyst", "description": "Data processing", "yoe": 2}
                ]
            }

        #Обработка PUT запроса
        elif msg_type == "put":
            #Болванка
            response = {
                "status": "updated",
                "vacancy": {
                    "id": body.get("vacancy_id"),
                    "name": body.get("name", "Unknown"),
                    "description": body.get("description", ""),
                    "yoe": body.get("yoe", 0)
                }
            }

        #Обработка чё-попало
        else:
            response = {"status": "error", "message": "Unknown message type"}
        
        return response
        
    except Exception as e:
        return {"status": "error", "message": str(e)}


#Обработка входящего сообщения и ответ на него
async def on_message(message: aio_pika.IncomingMessage, channel: AbstractRobustChannel):
    """Handle incoming message with RPC response"""
    print(f" [VACANCIES] Received: {message.body.decode()}")
    
    try:
        #Обработка
        response_body = await process_vacancy_request(message, channel)
        
        #Отправка ответа
        if message.reply_to:
            response_msg = Message(
                body=json.dumps(response_body).encode(),
                correlation_id=message.correlation_id,
                delivery_mode=2
            )
            await channel.default_exchange.publish(
                response_msg,
                routing_key=message.reply_to
            )
            print(f" [VACANCIES] Sent response to {message.reply_to}")
        
        await message.ack()
        
    except Exception as e:
        print(f" [VACANCIES] Error processing message: {e}")
        await message.nack(requeue=False)


async def main():
    connection: AbstractRobustConnection = None
    
    try:
        #Соединение с RabbitMQ
        connection = await connect_robust(
            "amqp://vacancies:vacancies@rabbitmq/",
            login='vacancies',
            password='vacancies'
        )
        
        channel: AbstractRobustChannel = await connection.channel()
        
        #Объявление очереди
        queue: AbstractRobustQueue = await channel.declare_queue(
            'vacancies_queue',
            durable=True,
            arguments={'x-queue-type': 'quorum'}
        )
        
        #Получение сообщений
        await queue.consume(lambda m: on_message(m, channel))
        
        print(' [*] Vacancies service waiting for messages. To exit press CTRL+C')
        await asyncio.Future()  #Править целую вечность
        
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception as e:
        print(f"Error: {e}")
        if connection:
            await connection.close()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
