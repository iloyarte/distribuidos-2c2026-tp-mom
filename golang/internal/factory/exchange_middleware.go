package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

type ExchangeMiddleware struct {
	exchangeName  string
	connector     *RabbitConnector
	exchangeQueue string
	routingKeys   []string
}

func NewExchangeMiddleware(connectionSettings m.ConnSettings, exchangeName string, keys []string) (*ExchangeMiddleware, error) {
	connector := NewRabbitConnector(connectionSettings)
	err := connector.declareExchange(exchangeName)
	if err != nil {
		return nil, err
	}
	queue, err := connector.declareQueue("", true, nil)
	if err != nil {
		return nil, err
	}

	return &ExchangeMiddleware{
		exchangeName:  exchangeName,
		connector:     connector,
		exchangeQueue: queue.Name,
		routingKeys:   keys,
	}, nil
}

func (eMiddleware *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	err := eMiddleware.bindQueues()
	if err != nil {
		return err
	}
	err = eMiddleware.connector.consumeQueue(eMiddleware.exchangeQueue, eMiddleware.exchangeName, callbackFunc)
	if err != nil {
		return err
	}
	return nil
}

func (eMiddleware *ExchangeMiddleware) bindQueues() error {
	for _, key := range eMiddleware.routingKeys {
		err := eMiddleware.connector.bindQueue(eMiddleware.exchangeQueue, key, eMiddleware.exchangeName)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (eMiddleware *ExchangeMiddleware) StopConsuming() error {
	return eMiddleware.connector.stopConsuming(eMiddleware.exchangeName)
}

func (eMiddleware *ExchangeMiddleware) Send(msg m.Message) error {
	for _, key := range eMiddleware.routingKeys {
		err := eMiddleware.connector.publish(msg, eMiddleware.exchangeName, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (eMiddleware *ExchangeMiddleware) Close() error {
	err := eMiddleware.connector.closeConnections()
	if err != nil {
		return err
	}
	return nil
}
