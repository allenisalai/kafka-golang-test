package main

import (
	"github.com/Shopify/sarama"
	"math/rand"
	"time"
	"fmt"
	"encoding/json"
	"github.com/allenisalai/kafka-golang-test/internal/pipe"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)



var client sarama.Client

func main() {
	producer := pipe.CreateProducer()
	defer producer.Close()

	for {
		time.Sleep(time.Duration(RandomNumber(1, 5)) * time.Second)
		fmt.Println("New message")

		assessment := pipe.Assessment{
			fmt.Sprintf("assessment_%v", RandomNumber(20, 1000)),
			"Student ID",
			map[string]string{
				"test": "aa",
			},
		}

		assessmentJson, err := json.Marshal(assessment)
		if err != nil {
			panic(err)
		}

		topic := pipe.TOPIC_ASSESSMENT;
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          assessmentJson,
		}, nil)


		if err != nil {
			fmt.Println(err.Error())
		}

	}

}


func RandomNumber(min int, max int) int {
	return rand.Intn(max-min) + min
}
