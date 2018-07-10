package pipe

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const TOPIC_ASSESSMENT string = "ASSESSMENT"

type Assessment struct {
	AssessmentId string
	StudentId    string
	Responses    map[string]string
}

func CreateProducer() *kafka.Producer {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	return p
}

func CreateConsumer() *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "test",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	return c
}