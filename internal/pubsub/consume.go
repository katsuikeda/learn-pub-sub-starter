package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
		handler,
		func(data []byte) (T, error) {
			buf := bytes.NewBuffer(data)
			decoder := gob.NewDecoder(buf)
			var content T
			err := decoder.Decode(&content)
			return content, err
		},
	)
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
		handler,
		func(data []byte) (T, error) {
			var content T
			err := json.Unmarshal(data, &content)
			return content, err
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("couldn't declare and bind queue: %w", err)
	}

	// Prefetch configuration
	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("couldn't set channel prefetch count: %w", err)
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
			content, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("Couldn't unmarshal message: %v", err)
				continue
			}

			ackType := handler(content)
			switch ackType {
			case Ack:
				if err := msg.Ack(false); err != nil {
					fmt.Printf("Couldn't acknowledge message: %v\n", err)
				}
			case NackRequeue:
				if err := msg.Nack(false, true); err != nil {
					fmt.Printf("Couldn't nack message: %v\n", err)
				}
			case NackDiscard:
				if err := msg.Nack(false, false); err != nil {
					fmt.Printf("Couldn't discard message: %v\n", err)
				}
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

	queue, err := ch.QueueDeclare(
		queueName,                             // name
		simpleQueueType == SimpleQueueDurable, // durable
		simpleQueueType != SimpleQueueDurable, // delete when unused
		simpleQueueType != SimpleQueueDurable, // exclusive
		false,                                 // no-wait
		amqp.Table{
			// This optional argument set the DLX for this queue
			"x-dead-letter-exchange": routing.ExchangePerilDeadLetter,
		}, // arguments
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
