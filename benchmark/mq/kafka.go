package mq

import (
	"github.com/Shopify/sarama"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Kafka struct {
	handler   benchmark.MessageHandler
	pubClient *sarama.Client
	subClient *sarama.Client
	pub       *sarama.Producer
	sub       *sarama.Consumer
	topic     string
}

func kafkaReceive(k *Kafka) {
	for {
		event := <-k.sub.Events()
		if k.handler.ReceiveMessage(event.Value) {
			break
		}
	}
}

func kafkaAsyncErrors(k *Kafka) {
	for _ = range k.pub.Errors() {
	}
}

func NewKafka(numberOfMessages int, testLatency bool) *Kafka {
	pubClient, _ := sarama.NewClient("pub", []string{"localhost:9092"}, sarama.NewClientConfig())
	subClient, _ := sarama.NewClient("sub", []string{"localhost:9092"}, sarama.NewClientConfig())

	topic := "test"
	pub, _ := sarama.NewProducer(pubClient, sarama.NewProducerConfig())
	consumerConfig := sarama.NewConsumerConfig()
	consumerConfig.OffsetMethod = sarama.OffsetMethodNewest // Only read new messages
	sub, _ := sarama.NewConsumer(subClient, topic, 0, "test", consumerConfig)

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
		handler:   handler,
		pubClient: pubClient,
		subClient: subClient,
		pub:       pub,
		sub:       sub,
		topic:     topic,
	}
}

func (k *Kafka) Setup() {
	go kafkaReceive(k)
	go kafkaAsyncErrors(k)
}

func (k *Kafka) Teardown() {
	k.pub.Close()
	k.sub.Close()
	k.pubClient.Close()
	k.subClient.Close()
}

func (k *Kafka) Send(message []byte) {
	k.pub.QueueMessage(k.topic, nil, sarama.ByteEncoder(message))
}

func (k *Kafka) MessageHandler() *benchmark.MessageHandler {
	return &k.handler
}
