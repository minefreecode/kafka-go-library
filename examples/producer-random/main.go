package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

// Возвращаем нового писателя в Кафку
func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	// get kafka writer using environment variables.
	kafkaURL := os.Getenv("kafkaURL")         //Урл кафки
	topic := os.Getenv("topic")               //типик кафки
	writer := newKafkaWriter(kafkaURL, topic) //писатель кафки
	defer writer.Close()                      //Опеределяем момент закрытия писателя кафки
	fmt.Println("start producing ... !!")

	//Объявляем бесконечный цикл записи в Кафку с инкрементами i
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i) //Создаем ключ для записи в Кафку
		msg := kafka.Message{
			Key:   []byte(key),                    //Ключ в кафку
			Value: []byte(fmt.Sprint(uuid.New())), //Значение uuid в кафку
		} //
		err := writer.WriteMessages(context.Background(), msg) //Записываем в Кафку сообщения
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}
		time.Sleep(1 * time.Second) //Временой интервал между записями в Кафку
	}
}
