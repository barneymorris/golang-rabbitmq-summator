package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

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

	if err != nil {
		failOnError(err, "Failed to declare the queue")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		time.Sleep(1 * time.Second)

		rnd := strconv.Itoa(rand.Intn(100) + 1)

		err = ch.PublishWithContext(ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing {
			  ContentType: "text/plain",
			  Body:        []byte(rnd),
		})
	
		
		if err != nil {
			failOnError(err, "Failed to publush with context")
		}

		parsed, _ := strconv.Atoi(rnd)

		fmt.Printf("Sent %d to \"random\" queue\n", parsed)
	}
}