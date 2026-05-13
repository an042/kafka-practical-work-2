from confluent_kafka import Producer
import json
import socket

# Конфигурация продюсера 
conf = {
    'bootstrap.servers': 'localhost:9092', 	    # Используем IP брокера (для примера localhost)
    'client.id': socket.gethostname(),          # Присваиваем продюсеру имя вашего хоста
    'acks': 'all',                              # Ждем подтверждения от всех реплик 
    'compression.type': 'none',                 # Можно изменить на 'gzip', 'snappy' и т.д.
    'retries': 5                                # Число попыток при ошибка
}

# Создаем  экземпляр продюсера, передаем ему конфигурацию 
producer = Producer(conf) 

# Callback-функция для обработки статуса доставки сообщений
def delivery_report(err, msg):
    if err is not None:
        # Вывод ошибки доставки
        print(f'Ошибка доставки: {err}')
    else:
        # Сообщение успешно доставлено и подтверждено брокером
        print(f'Доставлено в {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# Функция асинхронной отправки сообщения
def produce_async(topic, headers, key, value):

    # JSON сериализация данных value
    serialized_value = json.dumps(value).encode('utf-8')
        
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

