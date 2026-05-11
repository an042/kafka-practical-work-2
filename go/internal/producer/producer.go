package producer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/anetishin/kafka-practical-2/internal/message"
	"github.com/segmentio/kafka-go"
)

// Producer отправляет сообщения в Kafka-топик.
type Producer struct {
	writer *kafka.Writer
}

func New(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,

		// RequiredAcks: RequireAll = acks=-1 — гарантия At Least Once.
		// Брокер подтверждает запись только после того, как все синхронные реплики получили сообщение.
		RequiredAcks: kafka.RequireAll,

		// MaxAttempts — количество повторных попыток отправки при ошибке (параметр retries).
		MaxAttempts: 5,

		// Async: false — ждём подтверждения от брокера перед отправкой следующего сообщения.
		// Это обеспечивает гарантию доставки At Least Once.
		Async: false,

		// Balancer — распределяет сообщения по партициям по Round Robin.
		Balancer: &kafka.RoundRobin{},
	}
	return &Producer{writer: writer}
}

// Run запускает бесконечный цикл отправки сообщений в топик.
func (p *Producer) Run(ctx context.Context) {
	defer p.writer.Close()

	id := 1
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		event := message.Event{
			ID:        id,
			Text:      fmt.Sprintf("message-%d", id),
			Source:    "producer",
			CreatedAt: time.Now().UTC(),
		}

		data, err := message.Serialize(event)
		if err != nil {
			log.Printf("[producer] ошибка сериализации: %v", err)
			continue
		}

		err = p.writer.WriteMessages(ctx, kafka.Message{Value: data})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[producer] ошибка отправки: %v", err)
			time.Sleep(time.Second)
			continue
		}

		fmt.Printf("[producer] отправлено: %+v\n", event)
		id++
		time.Sleep(500 * time.Millisecond)
	}
}
