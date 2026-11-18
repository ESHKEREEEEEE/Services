import aio_pika
from aio_pika import connect_robust, Message, IncomingMessage
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue
import asyncio
import json
import sys
import os
import re
import uuid
from typing import Optional, Dict
from llama_cpp import Llama

from download_model import download_model

MODEL_PATH = os.getenv("LLM_MODEL_PATH", "/app/models/qwen3.5-4b-Q4_K_M.gguf")

#Функция, отключения размышлений для QWEN
def disable_qwen_thinking(llm: Llama):
    base_handler = llm._chat_handlers.get(llm.chat_format)

    if base_handler is None:
        raise RuntimeError(
            f"Не найден chat handler для формата {llm.chat_format}"
        )

    def no_thinking_handler(**kwargs):
        kwargs["enable_thinking"] = False
        return base_handler(**kwargs)

    llm.chat_handler = no_thinking_handler

#Генерация сопроводительного письма
async def generate_cover_letter(
    user_data: str,
    vacancy_data: str,
    llm: Llama
) -> str:

    fallback = (
        "Здравствуйте! Меня заинтересовала ваша вакансия. "
        "Мой профессиональный опыт и навыки соответствуют "
        "требованиям должности. Буду рад обсудить возможность "
        "сотрудничества на собеседовании."
    )

    if llm is None:
        return fallback

    try:
        messages = [
            {
                "role": "system",
                "content": """
                Ты — профессиональный HR-копирайтер.

                Составь сопроводительное письмо на вакансию от лица самого разработчика.

                Требования:
                - Только русский язык.
                - От 3 до 5 предложений.
                - Деловой и естественный стиль.
                - Используй только информацию из входных данных.
                - Подчеркни соответствие кандидата вакансии.
                - Не выдумывай навыки и достижения.
                - Не упоминай возраст.
                - Не выводи рассуждения и анализ.
                - Не добавляй заголовки и пояснения.

                Верни только готовый текст письма.
                                """
                            },
                            {
                                "role": "user",
                                "content": f"""
                Данные соискателя:
                {user_data}

                Данные вакансии:
                {vacancy_data}

                Составь краткое сопроводительное письмо.
                """
            }
        ]

        response = await asyncio.to_thread(
            llm.create_chat_completion,
            messages=messages,
            max_tokens=256,
            temperature=0.3,
            top_p=0.9,
            repeat_penalty=1.1
        )

        cover_letter = (
            response["choices"][0]["message"]["content"] or ""
        ).strip()

        #Удаление thinking, если есть
        if "</think>" in cover_letter:
            cover_letter = cover_letter.rsplit(
                "</think>", 1
            )[-1].strip()

        if re.match(
            r"(?is)^\s*(thinking process|analysis|reasoning)\s*[:\n]",
            cover_letter
        ):
            raise ValueError(
                "Model responded with reasoning"
            )

        cover_letter = re.sub(
            r"^(сопроводительное письмо|вот сопроводительное письмо)\s*:\s*",
            "",
            cover_letter,
            flags=re.IGNORECASE
        ).strip()

        if not cover_letter:
            raise ValueError("Пустой ответ модели")

        return cover_letter

    except Exception as e:
        print(f"LLM generation error: {e}")
        return fallback


#Функция для получения данных о пользователе по id
#Т.к. микросервис такой не реализован, то это просто заглушка
async def get_user_data(user_id: int):
    if (user_id == 1):
        return """{
            "name": "Жмышенко Валерий Альбертович",
            "age": 54,
            "position": "Senior Backend Developer",
            "experience": "18 лет",
            "location": "Санкт-Петербург",
            "skills": [
                "Python",
                "Django",
                "PostgreSQL",
                "Docker",
                "REST API",
                "Redis"
            ],
            "education": "Высшее техническое образование",
            "employment": "Полная занятость",
            "salary_expectation": "220 000 ₽",
            "about": "Опытный разработчик. Специализируется на создании серверных приложений и проектировании баз данных. Умеет работать самостоятельно и в команде.",
            "email": "valery.zhm@example.com"
            }"""
    else:
        return json.dumps({
                            "status": "error",
                            "message": "User not found"
                        })


#Глобальные параметри подключения микросервиса
vacancies_rpc_connection: Optional[AbstractRobustConnection] = None
vacancies_rpc_channel: Optional[AbstractRobustChannel] = None
vacancies_rpc_response_waiters: Dict[str, asyncio.Future] = {}
vacancies_rpc_queue: Optional[AbstractRobustQueue] = None
_vacancies_rpc_listener_task: Optional[asyncio.Task] = None


#Начать слушать 
async def _start_vacancies_rpc_listener():
    global _vacancies_rpc_listener_task, vacancies_rpc_queue
    
    if _vacancies_rpc_listener_task is None and vacancies_rpc_queue:
        _vacancies_rpc_listener_task = asyncio.create_task(_vacancies_rpc_listener())


#Прослушивание и отправка сообщений в обработку
async def _vacancies_rpc_listener():
    global vacancies_rpc_response_waiters
    
    if not vacancies_rpc_queue:
        return
    
    async with vacancies_rpc_queue.iterator() as queue_iter:
        async for message in queue_iter:
            await _handle_vacancies_rpc_response(message)


#Обработать возвращенный результат
async def _handle_vacancies_rpc_response(message: IncomingMessage):
    correlation_id = message.correlation_id
    if correlation_id and correlation_id in vacancies_rpc_response_waiters:
        try:
            body = json.loads(message.body.decode())
            vacancies_rpc_response_waiters[correlation_id].set_result(body)
        except Exception as e:
            vacancies_rpc_response_waiters[correlation_id].set_exception(e)
        finally:
            if correlation_id in vacancies_rpc_response_waiters:
                del vacancies_rpc_response_waiters[correlation_id]
    await message.ack()


#Подключение к RabbitMQ
async def _get_vacancies_rpc_connection():
    global vacancies_rpc_connection, vacancies_rpc_channel, vacancies_rpc_queue
    
    if vacancies_rpc_connection is None or vacancies_rpc_connection.is_closed:
        try:
            if vacancies_rpc_connection and not vacancies_rpc_connection.is_closed:
                await vacancies_rpc_connection.close()
            
            vacancies_rpc_connection = await connect_robust(
                "amqp://vacancies:vacancies@rabbitmq/",
                login='vacancies',
                password='vacancies'
            )
            vacancies_rpc_channel = await vacancies_rpc_connection.channel()
            
            vacancies_rpc_queue = await vacancies_rpc_channel.declare_queue(exclusive=True)
            
            await _start_vacancies_rpc_listener()
            
            print(" [COVER] Connected to vacancies RPC queue")
        except Exception as e:
            print(f" [COVER] Error connecting to vacancies RPC: {e}")
            raise
    
    return vacancies_rpc_connection, vacancies_rpc_channel


#Получение данных вакансии по id
async def get_vacancy_data(vacancy_id: int) -> str:
    global vacancies_rpc_response_waiters
    
    if not isinstance(vacancy_id, int) or vacancy_id <= 0:
        return json.dumps({
            "status": "error",
            "message": "Bad id"
        })
    
    try:
        connection, channel = await _get_vacancies_rpc_connection()

        #Будущее и id
        correlation_id = str(uuid.uuid4())
        response_future: asyncio.Future = asyncio.Future()
        vacancies_rpc_response_waiters[correlation_id] = response_future
        
        #Отправка сообщения
        payload = {
            "type": "get_id",
            "id": vacancy_id
        }
        
        message = Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            correlation_id=correlation_id,
            reply_to=vacancies_rpc_queue.name if vacancies_rpc_queue else "",
            delivery_mode=2
        )
        
        await channel.default_exchange.publish(
            message,
            routing_key='vacancies_queue'
        )
        
        print(f" [COVER] Sent RPC request to vacancies: {payload}")
        
        #Ожидание ответа
        try:
            result = await asyncio.wait_for(response_future, timeout=5.0)
            
            #Извлечение данных вакансии
            if not isinstance(result, dict):
                print(f" [COVER] Got unexpected response from vacancies: {result} (type: {type(result).__name__})")
                return json.dumps({
                    "status": "error",
                    "message": f"Invalid response format from vacancies service: expected dict, got {type(result).__name__}"
                })
            
            if result.get("status") == "success" and result.get("vacancies"):
                vacancy = result["vacancies"]
                if isinstance(vacancy, tuple):
                    vacancy = {
                        "id": vacancy[0],
                        "name": vacancy[1],
                        "description": vacancy[2],
                        "yoe": vacancy[3]
                    }
                elif isinstance(vacancy, list):
                    vacancy = {
                        "id": vacancy[0],
                        "name": vacancy[1],
                        "description": vacancy[2],
                        "yoe": vacancy[3] if len(vacancy) > 3 else None
                    }
                print(f" [COVER] Got reply from vacancies: {vacancy}")
                return json.dumps(vacancy)
            else:
                print(f" [COVER] Got reply from vacancies: NOT FOUND")
                return json.dumps({
                    "status": "error",
                    "message": result.get("message", "Vacancy not found"),
                })
                
        except asyncio.TimeoutError:
            if correlation_id in vacancies_rpc_response_waiters:
                del vacancies_rpc_response_waiters[correlation_id]
            return json.dumps({
                "status": "error",
                "message": "Timeout error"
            })
            
    except Exception as e:
        print(f" [COVER] Error getting vacancy data: {e}")
        return json.dumps({
            "status": "error",
            "message": str(e)
        })


#Обработка входящего запроса на генерацию
async def process_cover_request(message: aio_pika.IncomingMessage, channel: AbstractRobustChannel, llm: Llama) -> dict:
    try:
        body = json.loads(message.body.decode())
        user_id = body.get("user_id")
        vacancy_id = body.get("vacancy_id")
        
        if user_id is None or vacancy_id is None:
            return {
                "status": "error",
                "message": "Missing user_id or vacancy_id"
            }

        user_data_raw = await get_user_data(user_id)
        try:
            user_data_parsed = json.loads(user_data_raw)
            if isinstance(user_data_parsed, dict) and user_data_parsed.get("status") == "error":
                return user_data_parsed
            user_data = user_data_raw
        except json.JSONDecodeError:
            user_data = user_data_raw

        vacancy_data_raw = await get_vacancy_data(vacancy_id)
        try:
            vacancy_data_parsed = json.loads(vacancy_data_raw)
            if isinstance(vacancy_data_parsed, dict) and vacancy_data_parsed.get("status") == "error":
                return vacancy_data_parsed
            vacancy_data = vacancy_data_raw
        except json.JSONDecodeError:
            vacancy_data = vacancy_data_raw
        
        cover_letter = await generate_cover_letter(user_data, vacancy_data, llm)
        
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
async def on_message(message: aio_pika.IncomingMessage, channel: AbstractRobustChannel, llm: Llama):
    print(f" [COVER] Received: {message.body.decode()}")
    
    try:
        #Обработка
        response_body = await process_cover_request(message, channel, llm)
        
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


#Глобальные параметры подключения
connection: AbstractRobustConnection = None
channel: AbstractRobustChannel = None
queue: AbstractRobustQueue = None

async def main():
    global connection, channel, queue, vacancies_rpc_connection
    
    connection = None
    
    try:
        #Соединение с RabbitMQ
        connection = await connect_robust(
            "amqp://vacancies:vacancies@rabbitmq/",
            login='vacancies',
            password='vacancies'
        )
        
        channel = await connection.channel()
        
        #Объявление очереди
        queue = await channel.declare_queue(
            'cover_generator_queue',
            durable=True,
            arguments={'x-queue-type': 'quorum'}
        )

        #Установка модели если ее нет
        if not os.path.exists(MODEL_PATH):
            download_model()

        #Инициализация LLM
        try: 
            llm = Llama(
            model_path=MODEL_PATH,
            n_ctx=2048,
            n_gpu_layers=-1,
            verbose=False
            )

            disable_qwen_thinking(llm)

            print(f"LLM initialized: {llm.chat_format}")
        except ImportError:
            llm = None
            print("Warning: llama-cpp-python not installed, using fallback")
        except Exception as e:
            llm = None
            print(f"Warning: LLM initialization failed: {e}, using fallback")
        
        #Получение сообщений
        await queue.consume(lambda m: on_message(m, channel, llm))
        
        print(' [*] Cover generator waiting for messages. To exit press CTRL+C')
        await asyncio.Future() #Править целую вечность
        
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            #Закрыть соединение при прерывании
            if vacancies_rpc_connection and not vacancies_rpc_connection.is_closed:
                await vacancies_rpc_connection.close()
                print(" [COVER] RPC connection to vacancies closed")
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception as e:
        print(f"Error: {e}")
        if connection:
            await connection.close()
        #Закрыть соединение при ошибке
        if vacancies_rpc_connection and not vacancies_rpc_connection.is_closed:
            await vacancies_rpc_connection.close()
            print(" [COVER] RPC connection to vacancies closed")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
