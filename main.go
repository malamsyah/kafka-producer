package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	http.HandleFunc("/", handler)
	http.ListenAndServe(":9090", nil)
}

func handler(w http.ResponseWriter, r *http.Request) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "malamsyah.id:9092"})
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topics, ok := r.URL.Query()["topic"]

	if !ok || len(topics[0]) < 1 {
		log.Println("Url Param 'topic' is missing")
		return
	}

	topic := topics[0]

	for i := 0; i < 20; i++ {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(`{"foo":"bar"}`),
		}, nil)

		producer.Flush(15 * 1000)
		time.Sleep(500 * time.Millisecond)
	}
}
