package factory

import (
	"context"
	"fmt"
	"log"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConnector struct {
	connectionSettings m.ConnSettings
	channel            *amqp.Channel
	connection         *amqp.Connection
}

func NewRabbitConnector(settings m.ConnSettings) *RabbitConnector {
	connection, channel := openConnection(settings)
	return &RabbitConnector{
		connectionSettings: settings,
		connection:         connection,
		channel:            channel,
	}
}

func openConnection(settings m.ConnSettings) (*amqp.Connection, *amqp.Channel) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d", settings.Hostname, settings.Port)
	conn, err := amqp.Dial(url)

	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return conn, ch
}

func (rc *RabbitConnector) declareQueue(queueName string, autoDelete bool, args amqp.Table) (amqp.Queue, error) {
	queue, err := rc.channel.QueueDeclare(
		queueName,  // name
		true,       // durability
		autoDelete, // delete when unused
		false,      // exclusive
		false,      // no-wait
		args)
	failOnError(err, "Failed to declare a queue")
	return queue, nil
}

func (rc *RabbitConnector) declareExchange(exchangeName string) error {
	err := rc.channel.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		false,        // durability
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil
	}

	return nil
}

func (rc *RabbitConnector) bindQueue(queue string, key string, exchange string) error {
	err := rc.channel.QueueBind(queue, key, exchange, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func (rc *RabbitConnector) consumeQueue(
	queue string,
	consumer string,
	callback func(msg m.Message, ack func(), nack func()),
) error {
	queueChannel, err := rc.channel.Consume(
		queue,    // queue
		consumer, // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return err
	}
	go func() {
		for msg := range queueChannel {
			log.Printf("Received a message: %s", msg.Body)
			message := m.Message{
				Body: string(msg.Body),
			}
			callback(message, rc.ack(msg), rc.nack(msg))
		}
	}()
	return nil
}

func (rc *RabbitConnector) stopConsuming(consumerTag string) error {
	err := rc.channel.Cancel(consumerTag, false)
	if err != nil {
		return err
	}
	return nil
}

func (rc *RabbitConnector) publish(msg m.Message, exchange string, routingKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := rc.channel.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})

	if err != nil {
		return err
	}
	return nil
}

func (rc *RabbitConnector) closeConnections() error {
	err := rc.channel.Close()
	if err != nil {
		return err
	}
	err = rc.connection.Close()
	if err != nil {
		return err
	}
	return nil
}

func (rc *RabbitConnector) nack(msg amqp.Delivery) func() {
	return func() {
		err := msg.Nack(false, true)
		if err != nil {
			return
		}
	}
}

func (rc *RabbitConnector) ack(msg amqp.Delivery) func() {
	return func() {
		err := msg.Ack(false)
		if err != nil {
			return
		}
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
