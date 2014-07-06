package mq

import (
	"fmt"

	"github.com/streadway/amqp"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

type producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

type Rabbitmq struct {
	handler  benchmark.MessageHandler
	pub      *producer
	sub      *consumer
	queue    string
	exchange string
	key      string
}

func newConsumer(amqpUri, exchange, exchangeType, queueName, key, ctag string) (*consumer, error) {
	c := &consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error

	c.conn, err = amqp.Dial(amqpUri)
	if err != nil {
		return nil, err
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, err
	}

	if err = c.channel.ExchangeDeclare(
		exchange,
		exchangeType,
		false, // not durable
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	queue, err := c.channel.QueueDeclare(
		queueName,
		false, // not durable
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	if err = c.channel.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	return c, nil
}

func rabbitmqReceive(r Rabbitmq) {
loop:
	for {
		deliveries, _ := r.sub.channel.Consume(
			r.queue,
			r.sub.tag,
			false,
			false,
			false,
			false,
			nil,
		)

		for d := range deliveries {
			if r.ReceiveMessage(d.Body) {
				break loop
			}
		}
	}
}

func NewRabbitmq(numberOfMessages int, testLatency bool) Rabbitmq {
	exchange := "test"
	exchangeType := "direct"
	uri := "amqp://guest:guest@localhost:5672"
	queue := "test"
	key := "test-key"
	ctag := "tag"

	pubConn, _ := amqp.Dial(uri)
	pubChan, _ := pubConn.Channel()
	pubChan.ExchangeDeclare(
		exchange,
		exchangeType,
		false, // not durable
		false,
		false,
		false,
		nil,
	)
	pub := &producer{conn: pubConn, channel: pubChan}
	sub, _ := newConsumer(uri, exchange, exchangeType, queue, key, ctag)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return Rabbitmq{
		handler:  handler,
		pub:      pub,
		sub:      sub,
		queue:    queue,
		exchange: exchange,
		key:      key,
	}
}

func (r Rabbitmq) Setup() {
	go rabbitmqReceive(r)
}

func (r Rabbitmq) Teardown() {
	r.pub.conn.Close()
	r.sub.conn.Close()
}

func (r Rabbitmq) Send(message []byte) {
	r.pub.channel.Publish(
		r.exchange,
		r.key,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            message,
			DeliveryMode:    amqp.Transient,
			Priority:        0,
		},
	)
}

func (r Rabbitmq) ReceiveMessage(message []byte) bool {
	return r.handler.ReceiveMessage(message)
}

func (r Rabbitmq) MessageHandler() *benchmark.MessageHandler {
	return &r.handler
}
