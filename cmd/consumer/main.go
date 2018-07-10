package main

import (
	"github.com/allenisalai/kafka-golang-test/internal/pipe"
	"fmt"
	"encoding/json"
)

func main() {
	consumer := pipe.CreateConsumer()
	consumer.SubscribeTopics([]string{pipe.TOPIC_ASSESSMENT,}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {

			assessment := pipe.Assessment{}
			json.Unmarshal(msg.Value, &assessment)

			fmt.Printf("consuming: %v", assessment)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	consumer.Close()
}
