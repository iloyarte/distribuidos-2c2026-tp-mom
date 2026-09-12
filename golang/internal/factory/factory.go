package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	var connection, channel = connectToRabbit(connectionSettings)
	return NewQueueMiddleware(connection, channel, queueName), nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	var connection, channel = connectToRabbit(connectionSettings)
	return NewExchangeMiddleware(connection, channel, exchange, keys), nil
}

func connectToRabbit(settings m.ConnSettings) (*amqp.Connection, *amqp.Channel) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d", settings.Hostname, settings.Port)
	conn, err := amqp.Dial(url)

	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return conn, ch
}
