package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func newKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func copyMessages(sourceReader *kafka.Reader, destinationWriter *kafka.Writer, ctx context.Context) {
	for {
		msg, err := sourceReader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("failed to read message: %s", err)
		}
		fmt.Printf("received: %s\n", string(msg.Value))

		if err := destinationWriter.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("failed to write message: %s", err)
		} else {
			fmt.Println("Message copied to destination topic")
		}
	}
}

func getEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("ERROR: the environment variable %s must be set", key)
	}
	return value
}

func main() {
	sourceKafkaURL := getEnv("SOURCE_KAFKA_URL")
	sourceTopic := getEnv("SOURCE_TOPIC")
	groupID := getEnv("GROUP_ID")

	destinationKafkaURL := getEnv("DESTINATION_KAFKA_URL")
	destinationTopic := getEnv("DESTINATION_TOPIC")

	ctx := context.Background()

	sourceReader := newKafkaReader(sourceKafkaURL, sourceTopic, groupID)
	defer sourceReader.Close()

	destinationWriter := newKafkaWriter(destinationKafkaURL, destinationTopic)
	defer destinationWriter.Close()

	copyMessages(sourceReader, destinationWriter, ctx)
}
