package message

import (
	"encoding/json"
	"time"
)

// Event — структура сообщения, которое продюсер отправляет в топик.
type Event struct {
	ID        int       `json:"id"`
	Text      string    `json:"text"`
	Source    string    `json:"source"`
	CreatedAt time.Time `json:"created_at"`
}

// Serialize сериализует Event в JSON-байты перед отправкой в Kafka.
func Serialize(e Event) ([]byte, error) {
	return json.Marshal(e)
}

// Deserialize десериализует JSON-байты из Kafka обратно в Event.
func Deserialize(data []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(data, &e)
	return e, err
}
