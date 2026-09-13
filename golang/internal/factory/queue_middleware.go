package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	connector *RabbitConnector
	queueName string
}

func NewQueueMiddleware(connectionSettings m.ConnSettings, queueName string) m.Middleware {
	connector := NewRabbitConnector(connectionSettings)
	_, err := connector.declareQueue(
		queueName,
		false,
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		return nil
	}
	return &QueueMiddleware{
		connector: connector,
		queueName: queueName,
	}
}

func (qMiddleware QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	err := qMiddleware.connector.consumeQueue(qMiddleware.queueName, qMiddleware.queueName, callbackFunc)
	if err != nil {
		return err
	}
	return nil
}

func (qMiddleware QueueMiddleware) StopConsuming() error {
	return qMiddleware.connector.stopConsuming(qMiddleware.queueName)
}

func (qMiddleware QueueMiddleware) Send(msg m.Message) error {
	//TODO implement me
	panic("implement me")
}

func (qMiddleware QueueMiddleware) Close() error {
	err := qMiddleware.connector.closeConnections()
	if err != nil {
		return err
	}
	return nil
}
