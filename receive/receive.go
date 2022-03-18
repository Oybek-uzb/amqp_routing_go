package main

import (
	"github.com/streadway/amqp"
	"log"
	"os"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil)
	failOnError(err, "failed to declare a queue")

	if len(os.Args) < 2 {
		log.Printf("usage: %s, [info] [warning] [error]", os.Args[0])
	}

	for _, s := range os.Args[1:] {
		log.Printf(
			"binding queue %s to exchange %s with routing key %s",
			q.Name,
			"logs_direct",
			s)
		err := ch.QueueBind(
			q.Name,
			s,
			"logs_direct",
			false,
			nil)
		failOnError(err, "failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil)
	failOnError(err, "failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			log.Printf(" [*] Received: %s", msg.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
