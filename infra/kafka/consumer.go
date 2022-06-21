package kafka

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

type KafkaConsumer struct {
	MsgChan chan *ckafka.Message
}

func NewKafkaConsumer(msgChan chan *ckafka.Message) *KafkaConsumer {
	return &KafkaConsumer{
		MsgChan: msgChan,
	}
}

//criar um novo consumidor do kafka

//metodo de consumo

func (k *KafkaConsumer) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KafkaBootstrapServers"),
		"group.id":          os.Getenv("KafkaConsumerGroupId"),
	}

	c, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatalf("error consuming kafka message broker" + err.Error())
	}

	topics := []string{os.Getenv("KafkaReadTopic")}

	c.SubscribeTopics(topics, nil)
	fmt.Println("Kafka consumer has been started")

	//criacao de um loop infinito
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			//Enviando a mensagem para o canal definido na struct acima
			k.MsgChan <- msg
		}
	}
}
