# Практическая работа 2 — Kafka Producer + 2 Consumers

**Тема:** Настройка кластера и реализация продюсера с двумя консьюмерами  
**Языки:** Go (основной для сдачи), Java, Python  
**Библиотеки:** [segmentio/kafka-go](https://github.com/segmentio/kafka-go) · [kafka-clients](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients) · [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python)

---

## Структура проекта

```
.
├── go/                          # Go-реализация (основная)
│   ├── cmd/app/main.go
│   ├── internal/
│   │   ├── message/message.go
│   │   ├── producer/producer.go
│   │   └── consumer/
│   │       ├── single.go
│   │       └── batch.go
│   ├── go.mod
│   └── Dockerfile
│
├── java/                        # Java-реализация (Maven)
│   ├── src/main/java/com/anetishin/kafka/
│   │   ├── App.java
│   │   ├── model/KafkaEvent.java
│   │   ├── producer/EventProducer.java
│   │   └── consumer/
│   │       ├── SingleMessageConsumer.java
│   │       └── BatchMessageConsumer.java
│   ├── pom.xml
│   └── Dockerfile
│
├── python/                      # Python-реализация
│   ├── main.py
│   ├── message.py
│   ├── producer.py
│   ├── consumer_single.py
│   ├── consumer_batch.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── docker-compose.yml           # кластер из 3 брокеров + 2 экземпляра приложения (Go)
├── topic.txt                    # команды создания и описания топика
└── README.md
```

---

## Архитектура

Приложение состоит из трёх компонентов, запущенных параллельно в горутинах/потоках:

### Producer
Отправляет сообщения типа `Event` в топик `events` каждые 500 мс.

- **Гарантия доставки:** At Least Once
- **acks=all** — брокер подтверждает запись только после того, как все синхронные реплики получили сообщение
- **retries=5** — количество повторных попыток при ошибке
- **Sync** — продюсер ждёт подтверждения перед следующей отправкой
- Сериализация: JSON

### SingleMessageConsumer
Читает по **одному сообщению** за раз.

- **GroupID:** `single-consumer-group`
- Автокоммит оффсета включён

### BatchMessageConsumer
Читает минимум **10 сообщений** за один poll.

- **GroupID:** `batch-consumer-group`
- Автокоммит **отключён** — ручной коммит (`commitSync` / `commit`) одной операцией после пачки из 10
- **fetch.min.bytes = 1024** — брокер ждёт накопления данных перед ответом
- **fetch.max.wait.ms = 500** — максимальное ожидание данных

Оба консьюмера имеют **уникальные group_id** и работают **параллельно**, читая одни и те же сообщения независимо друг от друга.

---

## Запуск

### Требования
- Docker и Docker Compose

### 1. Запустить кластер и Go-приложение

```bash
docker compose up --build
```

Это поднимет:
- 3 брокера Kafka (kafka1, kafka2, kafka3) в KRaft-режиме
- сервис `kafka-setup`, который создаёт топик `events` (3 партиции, 2 реплики)
- 2 экземпляра Go-приложения с продюсером и обоими консьюмерами

### 2. Создать топик вручную (если используется существующий кластер)

```bash
kafka-topics.sh --create \
  --topic events \
  --partitions 3 \
  --replication-factor 2 \
  --bootstrap-server localhost:9092
```

### 3. Запустить Java или Python-вариант вместо Go

В `docker-compose.yml` замени секцию `build`:

```yaml
# Go (по умолчанию)
app:
  build: ./go

# Java
app:
  build: ./java

# Python
app:
  build: ./python
```

### 4. Остановить

```bash
docker compose down -v
```

---

## Проверка работы

### Продюсер отправляет сообщения
```
[producer] отправлено: {ID:1 Text:message-1 Source:producer CreatedAt:...}
[producer] отправлено: {ID:2 Text:message-2 Source:producer CreatedAt:...}
```

### SingleMessageConsumer читает по одному
```
[single-consumer] получено (partition=0, offset=0): {ID:1 ...}
[single-consumer] получено (partition=1, offset=0): {ID:2 ...}
```

### BatchMessageConsumer читает пачками по 10
```
[batch-consumer] обработано (partition=0, offset=0): {ID:1 ...}
...
[batch-consumer] закоммичена пачка из 10 сообщений
```

### Оба консьюмера получают одни и те же сообщения
Это происходит потому что у них разные `group_id`. Каждая группа независимо читает все партиции топика.

### 2 экземпляра приложения
При `replicas: 2` запускается два контейнера с одинаковым приложением. Консьюмеры из одной группы (например, два `single-consumer-group`) распределяют партиции между собой — при 3 партициях один получает 2, другой 1.

### Описание топика
```bash
kafka-topics.sh --describe --topic events --bootstrap-server localhost:9092
```
Ожидаемый вывод: 3 партиции, replication factor 2, лидеры распределены по брокерам.

---

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `KAFKA_BROKERS` | `kafka1:9092,kafka2:9092,kafka3:9092` | Список брокеров через запятую |
| `KAFKA_TOPIC` | `events` | Название топика |

---

## Сравнение реализаций по языкам

| | Go | Java | Python |
|---|---|---|---|
| Библиотека | segmentio/kafka-go | kafka-clients 3.7 | confluent-kafka-python |
| Продюсер (acks) | `RequireAll` | `"all"` | `"all"` |
| Retries | `MaxAttempts=5` | `retries=5` | `retries=5` |
| Single consumer | `ReadMessage()` + автокоммит | `enable.auto.commit=true`, `max.poll.records=1` | `enable.auto.commit=True` |
| Batch consumer | `FetchMessage()` + `CommitMessages()` | `enable.auto.commit=false` + `commitSync(offsets)` | `enable.auto.commit=False` + `commit(offsets)` |
| fetch.min.bytes | `MinBytes=1024` | `fetch.min.bytes=1024` | `fetch.min.bytes=1024` |
| fetch.max.wait | `MaxWait=500ms` | `fetch.max.wait.ms=500` | `fetch.max.wait.ms=500` |
