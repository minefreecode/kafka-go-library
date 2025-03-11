package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

// Обработчик сервера. Продюсер обрабтывает не напрямую а через сервер, асинхронным образом
func producerHandler(kafkaWriter *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}
		//Упаковываем тело сообщения в объект Message Кафки
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: body,
		}
		//Записывам Message в Кафку
		err = kafkaWriter.WriteMessages(req.Context(), msg)

		//Проверка была ли ошибка при записе
		if err != nil {
			wrt.Write([]byte(err.Error())) //Записываем сообщение об ошибке в ответ
			log.Fatalln(err)
		}
	})
}

// Получить Кафку "Записыватель"
func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL), //Адрес Кафки
		Topic:    topic,               //Топик
		Balancer: &kafka.LeastBytes{}, //Получаем имя балансировщика Кафки и записываем его сюда же
	}
}

func main() {
	// get kafka writer using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	//Получаем объект Writer связанный с Кафкой
	kafkaWriter := getKafkaWriter(kafkaURL, topic)

	//Предварительно объявляем о закрытии Кафки
	defer kafkaWriter.Close()

	// Создаем сервер который отвечает сообщениями
	// Чтобы добавить асинхронности в работу продюсера
	http.HandleFunc("/", producerHandler(kafkaWriter))

	// Run the web server.
	fmt.Println("start producer-api ... !!")

	//Запускаем сервер и ждем ответа. таким образом продючем упакован в асинхронный сервер
	log.Fatal(http.ListenAndServe(":8080", nil))
}
