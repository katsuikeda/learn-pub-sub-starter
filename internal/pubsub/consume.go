package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("couldn't declare and bind queue: %w", err)
	}

	deliveryChan, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-Local
		false,      // no-Wait
		nil,        //args
	)
	if err != nil {
		return fmt.Errorf("couldn't start consuming: %w", err)
	}

	go func() {
		for msg := range deliveryChan {
			var target T
			if err := json.Unmarshal(msg.Body, &target); err != nil {
				fmt.Printf("Couldn't unmarshal message: %v\n", err)
				continue
			}

			handler(target)
			if err := msg.Ack(false); err != nil {
				fmt.Printf("Couldn't acknowledge message: %v\n", err)
			}
		}
	}()

	return nil
}

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
