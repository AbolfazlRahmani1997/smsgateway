package initx

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func LoadConfigFromJSON(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var data map[string]string
	if err := json.NewDecoder(f).Decode(&data); err != nil {
		return err
	}
	for k, v := range data {
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
	return nil
}

func BrokersFromEnv() []string {
	return strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
}

func NewReader(brokers []string, groupID, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: time.Second,
	})
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Murmur2Balancer{},
		RequiredAcks:           kafka.RequireAll,
		BatchTimeout:           5 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
}
