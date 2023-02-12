package main

import (
	"fmt"
	"log"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
	  log.Panicf("%s: %s", msg, err)
	}
}

  
func connect() *amqp.Connection {
	conn, err := amqp.Dial("amqp://myuser:mypassword@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	

	return conn
}

func main() {
	ch, err := connect().Channel()

	if err != nil {
		failOnError(err, "Failed to open channel")
	}

	defer ch.Close()
	
	fmt.Printf("Successfully connected to RabbitMQ\n")

	q, err := ch.QueueDeclare(
		"random", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	failOnError(err, "Failed to declare the queue")

	
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		true,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		sum := 0;

		for d := range msgs {
			value, err := strconv.Atoi(string(d.Body))
			if err != nil {
				failOnError(err, "Failed to parse consumed message")
			}

			sum += value

			log.Printf("New sum: %d", sum)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}