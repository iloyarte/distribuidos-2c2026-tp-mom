package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewExchangeMiddleware(conn *amqp.Connection, channel *amqp.Channel, exchangeName string, keys []string) *ExchangeMiddleware {
	err := channel.ExchangeDeclare(
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
	return &ExchangeMiddleware{
		conn,
		channel,
	}
}

func (eMiddleware ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	//TODO implement me
	panic("implement me")
}

func (eMiddleware ExchangeMiddleware) StopConsuming() error {
	//TODO implement me
	panic("implement me")
}

func (eMiddleware ExchangeMiddleware) Send(msg m.Message) error {
	//TODO implement me
	panic("implement me")
}

func (eMiddleware ExchangeMiddleware) Close() error {
	err := eMiddleware.conn.Close()
	if err != nil {
		return err
	}
	err = eMiddleware.channel.Close()
	if err != nil {
		return err
	}
	return nil
}
