package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")    // Получаем брокеры сообщений из строки. Можно передавать несколько брокеров
	return kafka.NewReader(kafka.ReaderConfig{ //Новое чтение
		Brokers:  brokers, //массив брокеров
		GroupID:  groupID, //Группа потребителей
		Topic:    topic,   //Топик из которого читаем
		MinBytes: 10e3,    // 10KB
		MaxBytes: 10e6,    // 10MB
	})
}

func main() {
	// get kafka reader using environment variables.
	//Из окружения env получаем параметры
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := os.Getenv("groupID")
	//Делаем чтение из Kafka
	reader := getKafkaReader(kafkaURL, topic, groupID)
	// Закрываем reader когда выполнятся все процессы
	defer reader.Close()

	//Вывод сообщения на экран
	fmt.Println("start consuming ... !!")
	//Бесконечный цикл на чтение
	for {
		m, err := reader.ReadMessage(context.Background()) //Чтение сообщения из Kafka
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
