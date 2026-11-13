import aio_pika
from aio_pika import connect_robust, Message
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue
import asyncio
import json
import sys
import os

#Генерация сопроводительного письма
async def generate_cover_letter(user_id: int, vacancy_id: int) -> str:
    #Сейчас возвращает заготовку   
    return  f"""Дорогие друзья, дорогих друзей,
    Очень хочу работать на должности, указанной в вакансии {vacancy_id}
    Я реально крут, мой id {user_id}
    """

#Обработка входящего запроса на генерацию
async def process_cover_request(message: aio_pika.IncomingMessage, channel: AbstractRobustChannel) -> dict:
    
    try:
        body = json.loads(message.body.decode())
        user_id = body.get("user_id")
        vacancy_id = body.get("vacancy_id")
        
        if user_id is None or vacancy_id is None:
            return {
                "status": "error",
                "message": "Missing user_id or vacancy_id"
            }
        
        cover_letter = await generate_cover_letter(user_id, vacancy_id)
        
        response = {
            "status": "success",
            "user_id": user_id,
            "vacancy_id": vacancy_id,
            "cover_letter": cover_letter
        }
        
        return response
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

#Обработка входящего сообщения и ответ на него
async def on_message(message: aio_pika.IncomingMessage, channel: AbstractRobustChannel):
    print(f" [COVER] Received: {message.body.decode()}")
    
    try:
        #Обработка
        response_body = await process_cover_request(message, channel)
        
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
            print(f" [COVER] Sent response to {message.reply_to}")
        
        await message.ack()
        
    except Exception as e:
        print(f" [COVER] Error processing message: {e}")
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
            'cover_generator_queue',
            durable=True,
            arguments={'x-queue-type': 'quorum'}
        )
        
        #Получение сообщений
        await queue.consume(lambda m: on_message(m, channel))
        
        print(' [*] Cover generator waiting for messages. To exit press CTRL+C')
        await asyncio.Future() #Править целую вечность
        
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
