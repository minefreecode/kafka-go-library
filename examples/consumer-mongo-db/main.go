package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/mongodb/mongo-go-driver/mongo"
	kafka "github.com/segmentio/kafka-go"
)

func getMongoCollection(mongoURL, dbName, collectionName string) *mongo.Collection {
	client, err := mongo.Connect(context.Background(), mongoURL)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.Background(), nil) //Проверяем на доступность сервера моного
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB ... !!")

	db := client.Database(dbName)               //Получаем объект базы данных из монги
	collection := db.Collection(collectionName) //Получаем коллекции из монго
	return collection
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL}, //Брокеры
		GroupID:  groupID,            //Группа потребителей-консумеров
		Topic:    topic,              //Топик
		MinBytes: 10e3,               // 10KB
		MaxBytes: 10e6,               // 10MB
	})
}

func main() {

	// get Mongo db Collection using environment variables.
	// Получение информации из окружения
	mongoURL := os.Getenv("mongoURL")                                  //Url mongo
	dbName := os.Getenv("dbName")                                      // Имя базы данных Монго
	collectionName := os.Getenv("collectionName")                      //Коллекция
	collection := getMongoCollection(mongoURL, dbName, collectionName) //Получение объекта коллекции из Монго

	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := os.Getenv("groupID")
	reader := getKafkaReader(kafkaURL, topic, groupID) //Читатель из Кафки

	defer reader.Close() //Объявляем закрытие

	fmt.Println("start consuming ... !!")

	//Бесконечный цикл по кафке
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err) //Логгирование о фатальной ошибке при чтении из Кафки
		}
		//Вставка в коллекции монги информации из Kafka в виде нового документа коллекуии
		insertResult, err := collection.InsertOne(context.Background(), msg)
		if err != nil {
			log.Fatal(err)
		}
		//Печать информации о вставке нового документа
		fmt.Println("Inserted a single document: ", insertResult.InsertedID)
	}
}
