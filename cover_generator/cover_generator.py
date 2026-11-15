import aio_pika
from aio_pika import connect_robust, Message
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue
import asyncio
import json
import sys
import os
import re
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


#Функция для получения данных о вакансии по id
#Заглушка
async def get_vacancy_data(vacancy_id: int):
    return """{
  "name": "Senior Python developer",
  "description": "Требуется разработчик программного обеспечения с опытом проектирования высоконагруженных систем.
  Условия: Моментальная выплата, релокационный пакет, стабильная зарплата, социальные гарантии",
  "yoe": 5
}"""


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

        user_data = await get_user_data(user_id)
        vacancy_data = await get_vacancy_data(vacancy_id)
        
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
