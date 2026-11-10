import pika, sys, os
from pika.credentials import PlainCredentials

def main():

    credentials = PlainCredentials(username = 'vacancies', password = 'vacancies', erase_on_connect=True)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost', port = 5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='cover_generator_queue', durable=True, arguments={'x-queue-type': 'quorum'})

    #Получение сообщения. Логика будет реализована в дальнейшем
    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")

    channel.basic_consume(queue='cover_generator_queue', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)