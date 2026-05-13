from confluent_kafka import Producer
import json
import socket

# Конфигурация продюсера 
conf = {
    'bootstrap.servers': 'localhost:9092', 	    # Используем IP брокера (для примера localhost)
    'client.id': socket.gethostname(),          # Присваиваем продюсеру имя вашего хоста
    'acks': 'all',                              # Ждем подтверждения от всех реплик 
    'compression.type': 'none',                 # Можно изменить на 'gzip', 'snappy' и т.д.
    'retries': 5                                # Число попыток при ошибке
}

# Создаем  экземпляр продюсера, передаем ему конфигурацию 
producer = Producer(conf) 

# Callback-функция для обработки статуса доставки сообщений
def delivery_report(err, msg):
    if err is not None:
        # Вывод ошибки доставки
        print(f'Ошибка доставки: {err}')
        # Добавление ошибки в лог
        with open("./kafka_yandex/Module1/logfile_Producer.txt", "a", encoding="utf-8") as file_test:
            print(f"[{consumer_id}] Ошибка доставки: {msg.error()}", file=file_test)
            file_test.close()
    else:
        # Сообщение успешно доставлено и подтверждено брокером
        print(f'Доставлено в {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# Функция асинхронной отправки сообщения
def produce_async(topic, headers, key, value):

    # JSON сериализация данных value
    try:
        serialized_value = json.dumps(value).encode('utf-8')
    except TypeError as e:
        # Добавление ошибки в лог
        with open("./kafka_yandex/Module1/logfile_Producer.txt", "a", encoding="utf-8") as file_test:
            print(f"Ошибка типа данных: {str(e)}", file=file_test)
            file_test.close()
        # print(f"Ошибка типа данных: {str(e)}")
    except Exception as e:
        # Добавление ошибки в лог
        with open("./kafka_yandex/Module1/logfile_Producer.txt", "a", encoding="utf-8") as file_test:
            print(f"Непредвиденная ошибка: {str(e)}", file=file_test)
            file_test.close()
        # print(f"Непредвиденная ошибка: {str(e)}")        
    # Отправка сообщения
    producer.produce(
        topic=topic,               # Укажем топик
        key=key,                   # Укажем ключ
        value=serialized_value,    # Добавляем явно преобразованное значение
        headers=headers,           # Добавляем заголовки
        callback=delivery_report
    )

# Подготавливаем сообщение
message_topic = 'test_topic_1'
message_headers = [
    ("source", "python-producer"),
    ("version", "0.1"),
    ("content-type", "text/plain")
]
message_key = 'synch_123'
message_value = {'data_1': 123, 'data_2': 'ОК'}

# Отправляем сообщения
# Сообщение 1
# produce_async(message_topic, message_headers, message_key, message_value)
# Сообщение 2
# produce_async(message_topic, message_headers, message_key, message_value)
for i in range(100):
    message_value = {'data_1': i, 'data_2': 'ОК'}
    produce_async(message_topic, message_headers, message_key, message_value)
    
# Завершаем работу: ждём, пока сообщения из буфера будут отправлены и обработаны колбэками.
# Затем закрываем соединения и освобождаем ресурсы
producer.flush()

