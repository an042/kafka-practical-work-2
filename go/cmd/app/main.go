package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/anetishin/kafka-practical-2/internal/consumer"
	"github.com/anetishin/kafka-practical-2/internal/producer"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092"), ",")
	topic := getEnv("KAFKA_TOPIC", "events")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Перехватываем сигналы завершения для graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("получен сигнал завершения, останавливаемся...")
		cancel()
	}()

	log.Printf("запуск приложения: brokers=%v, topic=%s", brokers, topic)

	// Продюсер отправляет сообщения в топик.
	p := producer.New(brokers, topic)
	go p.Run(ctx)

	// SingleMessageConsumer читает по одному сообщению с автокоммитом.
	single := consumer.NewSingle(brokers, topic)
	go single.Run(ctx)

	// BatchMessageConsumer читает пачками по 10+ сообщений с ручным коммитом.
	batch := consumer.NewBatch(brokers, topic)
	go batch.Run(ctx)

	<-ctx.Done()
	log.Println("приложение остановлено")
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
