import aio_pika
from aio_pika import connect_robust, Message
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue
import asyncio
import json
import sys
import os

from database import init_db, post_vacancy, put_vacancy, get_vacancies_by_yoe, get_vacancy_by_id

DB_FILE = "vacancies.db"


#Обработка запросов
async def process_vacancy_request(message: aio_pika.IncomingMessage, channel: AbstractRobustChannel) -> dict:
    try:
        body = json.loads(message.body.decode())
        msg_type = body.get("type", "unknown")

        #Обработка POST запроса
        if msg_type == "post":

            #Преобразование yoe
            try:
                yoe_value = int(body.get("yoe", 0))
            except (ValueError, TypeError):
                yoe_value = 0

            #Обращение к базе данных
            result_id = post_vacancy(DB_FILE, body.get("name", "unknown"), body.get("description", "unknown"), yoe_value)

            #Проверка результата
            if result_id > 0:
                response = {
                    "status": "created",
                    "vacancy": {
                        "id": result_id,
                        "name": body.get("name", "Unknown"),
                        "description": body.get("description", ""),
                        "yoe": yoe_value
                    }
                }
            else:
                response = {
                    "status": "error",
                    "message": "Failed to insert vacancy"
                }

        #Обработка GET запроса
        elif msg_type == "get_yoe":
            #Преобразование yoe
            try:
                yoe_value = int(body.get("yoe", 0))
            except (ValueError, TypeError):
                yoe_value = 0

            #Обращение к базе данных
            result = get_vacancies_by_yoe(DB_FILE, yoe_value)

            #Даже если ничего не нашлось - считаем это успешным результатом,
            response = {
                "status": "success",
                "vacancies": result
            }

        elif msg_type == "get_id":
            #Преобразование id
            try:
                id_value = int(body.get("id", 0))
            except (ValueError, TypeError):
                id_value = 0
        
            #Обращение к базе данных
            result = get_vacancy_by_id(DB_FILE, id_value)
        
            #Даже если ничего не нашлось - считаем это успешным результатом,
            response = {
                "status": "success",
                "vacancies": result
            }

        #Обработка PUT запроса
        elif msg_type == "put":
            #Получение vacancu_id
            id = body.get("vacancy_id")
            if id is None:
                response = {
                    "status": "error",
                    "message": "Failed to get id of vacancy"
                }
            else:
                #Преобразование id
                try:
                    id = int(id)
                except (ValueError, TypeError):
                    response = {
                        "status": "error",
                        "message": "Invalid id format"
                    }
                    id = None

                if id is not None:
                    #Преобразование yoe
                    try:
                        yoe_value = int(body.get("yoe", 0))
                    except (ValueError, TypeError):
                        yoe_value = 0

                    #Обращение к базе данных
                    result_id = put_vacancy(DB_FILE, id, body.get("name", "unknown"), body.get("description", "unknown"), yoe_value)

                    #Проверка результата
                    if result_id > 0:
                        response = {
                            "status": "updated",
                            "vacancy": {
                                "id": result_id,
                                "name": body.get("name", "Unknown"),
                                "description": body.get("description", ""),
                                "yoe": yoe_value
                            }
                        }
                    else:
                        response = {
                            "status": "error",
                            "message": "Failed to update vacancy"
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

        init_db(DB_FILE)

        
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
