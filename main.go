package main

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	level, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))

	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(level)
	}
}

func newKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func newKafkaWriter(kafkaURL string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Balancer: &kafka.LeastBytes{},
	}
}

func copyMessages(sourceReader *kafka.Reader, destinationWriter *kafka.Writer, ctx context.Context) {
	for {
		msg, err := sourceReader.ReadMessage(ctx)
		if err != nil {
			log.WithFields(log.Fields{
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"error":     err,
			}).Fatal("Failed to read message")
		}
		log.WithFields(log.Fields{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.Offset,
		}).Info("Message received")

		if err := destinationWriter.WriteMessages(ctx, msg); err != nil {
			log.WithFields(log.Fields{
				"topic": msg.Topic,
				"value": string(msg.Value),
				"error": err,
			}).Fatal("Failed to write message")
		}

		log.WithFields(log.Fields{
			"topic": msg.Topic,
			"value": string(msg.Value),
		}).Info("Message copied to destination topic")
	} // for
}

func getEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("ERROR: the environment variable %s must be set", key)
	}
	return value
}

func createTopics(kafkaURL string, topics []string) error {
	log.WithFields(log.Fields{
		"kafkaURL": kafkaURL,
		"topics":   topics,
	}).Info("Creating topics...")

	conn, err := kafka.Dial("tcp", kafkaURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	topicConfigs := make([]kafka.TopicConfig, len(topics))
	for i, topic := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 1,
		}
	}

	// Attempt to create all topics
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	return nil
}

func copycat_run(ctx context.Context, groupID, sourceKafkaURL, sourceTopic, destinationKafkaURL string, wg *sync.WaitGroup) {
	defer wg.Done()

	log.WithFields(log.Fields{
		"sourceKafkaURL":      sourceKafkaURL,
		"sourceTopic":         sourceTopic,
		"groupID":             groupID,
		"destinationKafkaURL": destinationKafkaURL,
	}).Info("Starting Kafka CopyCat...")

	sourceReader := newKafkaReader(sourceKafkaURL, sourceTopic, groupID)
	defer sourceReader.Close()

	destinationWriter := newKafkaWriter(destinationKafkaURL)
	defer destinationWriter.Close()

	copyMessages(sourceReader, destinationWriter, ctx)
}

func main() {
	ctx := context.Background()

	sourceKafkaURL := getEnv("SOURCE_KAFKA_URL")
	sourceTopics := strings.Fields(getEnv("SOURCE_TOPICS"))
	groupID := getEnv("GROUP_ID")
	destinationKafkaURL := getEnv("DESTINATION_KAFKA_URL")

	createTopics(destinationKafkaURL, sourceTopics)

	var wg sync.WaitGroup

	for _, sourceTopic := range sourceTopics {
		wg.Add(1)
		go copycat_run(ctx, groupID, sourceKafkaURL, sourceTopic, destinationKafkaURL, &wg)
	}

	wg.Wait()
}
