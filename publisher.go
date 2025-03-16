package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var kafkaTopic string = "harsh-topic"

func main() {

	// Create a kafka producer/publisher
	publisher, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer %s", err)
	}
	defer publisher.Close()

	// Handle delivery reports
	go func() {
		for e := range publisher.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topics %s partitions [%d] @ offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// list of messages
	messageTexts := []string{
		`{"orderId": 11, "status": "success"}`,
		`{"orderId": 12, "status": "failure"}`,
		`{"orderId": 13, "status": "pending"}`,
	}

	// publish each message in sequence
	for i := 0; i < len(messageTexts); i++ {
		messageValue := messageTexts[i]
		err := publisher.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(messageValue),
		}, nil)

		if err != nil {
			log.Printf("Failed to publish message: %v due to error: ", err)
		}
	}

	// Flush and wait for outstanding messages
	publisher.Flush(15 * 1000)

}
