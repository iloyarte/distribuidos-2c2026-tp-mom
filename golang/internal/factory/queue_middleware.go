package factory

import (
	"log"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewQueueMiddleware(conn *amqp.Connection, channel *amqp.Channel, queueName string) m.Middleware {
	queue, err := channel.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	failOnError(err, "Failed to declare a queue")

	return &QueueMiddleware{
		conn:    conn,
		channel: channel,
		queue:   queue,
	}
}

func (qMiddleware QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := qMiddleware.channel.Consume(
		qMiddleware.queue.Name, // queue
		"",                     // consumer
		true,                   // auto-ack
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)
	failOnError(err, "Failed to register a consumer")
	var forever chan struct{}

	go func() {
		for msg := range msgs {
			log.Printf("Received a message: %s", msg.Body)
			message := m.Message{
				Body: string(msg.Body),
			}
			callbackFunc(message, func() { msg.Ack(false) }, func() { msg.Nack(false, true) })
		}
	}()
	<-forever
	return nil
}

func (qMiddleware QueueMiddleware) StopConsuming() error {
	//TODO implement me
	panic("implement me")
}

func (qMiddleware QueueMiddleware) Send(msg m.Message) error {
	//TODO implement me
	panic("implement me")
}

func (qMiddleware QueueMiddleware) Close() error {
	err := qMiddleware.conn.Close()
	if err != nil {
		return err
	}
	err = qMiddleware.channel.Close()
	if err != nil {
		return err
	}
	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
