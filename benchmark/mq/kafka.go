package mq

import (
	"github.com/Shopify/sarama"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Kafka struct {
	handler benchmark.MessageHandler
	client  sarama.Client
	pub     sarama.AsyncProducer
	sub     sarama.PartitionConsumer
	topic   string
}

func kafkaReceive(k *Kafka) {
	for {
		msg := <-k.sub.Messages()
		if k.handler.ReceiveMessage(msg.Value) {
			break
		}
	}
}

func kafkaAsyncErrors(k *Kafka) {
	for _ = range k.pub.Errors() {
	}
}

func NewKafka(numberOfMessages int, testLatency bool) *Kafka {
	config := sarama.NewConfig()
	client, _ := sarama.NewClient([]string{"localhost:9092"}, config)

	topic := "test"
	pub, _ := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	consumer, _ := sarama.NewConsumerFromClient(client)
	sub, _ := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Kafka{
		handler: handler,
		client:  client,
		pub:     pub,
		sub:     sub,
		topic:   topic,
	}
}

func (k *Kafka) Setup() {
	go kafkaReceive(k)
	go kafkaAsyncErrors(k)
}

func (k *Kafka) Teardown() {
	k.pub.Close()
	k.sub.Close()
	k.client.Close()
}

func (k *Kafka) Send(message []byte) {
	k.pub.Input() <- &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   nil,
		Value: sarama.ByteEncoder(message),
	}
}

func (k *Kafka) MessageHandler() *benchmark.MessageHandler {
	return &k.handler
}
