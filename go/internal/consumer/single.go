package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/anetishin/kafka-practical-2/internal/message"
	"github.com/segmentio/kafka-go"
)

// SingleMessageConsumer читает по одному сообщению за раз с автоматическим коммитом оффсета.
type SingleMessageConsumer struct {
	reader *kafka.Reader
}

func NewSingle(brokers []string, topic string) *SingleMessageConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,

		// GroupID — уникальная группа консьюмера. Позволяет читать те же сообщения,
		// что и BatchMessageConsumer, независимо от него.
		GroupID: "single-consumer-group",

		// MinBytes/MaxBytes — минимальный и максимальный объём данных за один запрос к брокеру.
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &SingleMessageConsumer{reader: reader}
}

// Run запускает бесконечный цикл чтения сообщений по одному.
func (c *SingleMessageConsumer) Run(ctx context.Context) {
	defer c.reader.Close()

	for {
		// ReadMessage читает одно сообщение и автоматически коммитит оффсет.
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[single-consumer] ошибка чтения: %v", err)
			time.Sleep(time.Second)
			continue
		}

		event, err := message.Deserialize(msg.Value)
		if err != nil {
			log.Printf("[single-consumer] ошибка десериализации: %v", err)
			continue
		}

		fmt.Printf("[single-consumer] получено (partition=%d, offset=%d): %+v\n",
			msg.Partition, msg.Offset, event)
	}
}
