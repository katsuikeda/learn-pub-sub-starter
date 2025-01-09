package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("couldn't create channel: %w", err)
	}

	isDurable := simpleQueueType == SimpleQueueDurable
	isAutoDelete := simpleQueueType != SimpleQueueDurable
	isExclusive := simpleQueueType != SimpleQueueDurable

	queue, err := ch.QueueDeclare(
		queueName,    // name
		isDurable,    // durable
		isAutoDelete, // delete when unused
		isExclusive,  // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("couldn't declare queue: %w", err)
	}

	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("couldn't bind queue: %w", err)
	}

	return ch, queue, nil
}
