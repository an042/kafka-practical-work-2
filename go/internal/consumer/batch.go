package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/anetishin/kafka-practical-2/internal/message"
	"github.com/segmentio/kafka-go"
)

const batchSize = 10

// BatchMessageConsumer читает минимум 10 сообщений за один poll
// и коммитит оффсет один раз после обработки всей пачки.
type BatchMessageConsumer struct {
	reader *kafka.Reader
}

func NewBatch(brokers []string, topic string) *BatchMessageConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,

		// GroupID — уникальная группа, независимая от SingleMessageConsumer.
		// Обе группы получают одни и те же сообщения из топика.
		GroupID: "batch-consumer-group",

		// MinBytes — аналог fetch.min.bytes. Брокер ждёт накопления этого объёма данных
		// перед ответом консьюмеру, что помогает собирать пачки.
		MinBytes: 1024,

		// MaxWait — аналог fetch.max.wait.ms. Максимальное время ожидания данных от брокера.
		// Если данных недостаточно, брокер вернёт пустой ответ через это время.
		MaxWait: 500 * time.Millisecond,

		MaxBytes: 10e6,

		// CommitInterval: 0 — отключаем автоматический коммит.
		// Коммит выполняется вручную после обработки всей пачки.
		CommitInterval: 0,
	})
	return &BatchMessageConsumer{reader: reader}
}

// Run запускает цикл: собирает пачку из batchSize сообщений, обрабатывает, коммитит.
func (c *BatchMessageConsumer) Run(ctx context.Context) {
	defer c.reader.Close()

	for {
		batch := make([]kafka.Message, 0, batchSize)

		// Собираем пачку минимум из batchSize сообщений.
		for len(batch) < batchSize {
			// FetchMessage читает сообщение БЕЗ автоматического коммита оффсета.
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[batch-consumer] ошибка получения: %v", err)
				time.Sleep(time.Second)
				break
			}
			batch = append(batch, msg)
		}

		if len(batch) == 0 {
			continue
		}

		// Обрабатываем каждое сообщение в пачке.
		for _, msg := range batch {
			event, err := message.Deserialize(msg.Value)
			if err != nil {
				log.Printf("[batch-consumer] ошибка десериализации: %v", err)
				continue
			}
			fmt.Printf("[batch-consumer] обработано (partition=%d, offset=%d): %+v\n",
				msg.Partition, msg.Offset, event)
		}

		// Один коммит оффсета для всей пачки после её полной обработки.
		if err := c.reader.CommitMessages(ctx, batch...); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[batch-consumer] ошибка коммита: %v", err)
			continue
		}
		fmt.Printf("[batch-consumer] закоммичена пачка из %d сообщений\n", len(batch))
	}
}
