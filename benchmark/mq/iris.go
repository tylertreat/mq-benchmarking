package mq

import (
	"runtime"

	"github.com/tylertreat/mq-benchmarking/benchmark"
	"gopkg.in/project-iris/iris-go.v1"
)

type Iris struct {
	handler benchmark.MessageHandler
	topic   string
	pub     *iris.Connection
	sub     *iris.Connection
}

type EventHandler struct {
	handler benchmark.MessageHandler
}

func (t *EventHandler) HandleEvent(event []byte) {
	t.handler.ReceiveMessage(event)
}

func NewIris(numberOfMessages int, testLatency bool) *Iris {
	topic := "test"
	pub, _ := iris.Connect(55555)
	sub, _ := iris.Connect(55555)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	sub.Subscribe(topic, &EventHandler{handler}, &iris.TopicLimits{
		EventThreads: runtime.NumCPU(),
		EventMemory:  1024 * 1024,
	})

	return &Iris{
		handler: handler,
		topic:   topic,
		pub:     pub,
		sub:     sub,
	}
}

func (i *Iris) Setup() {}

func (i *Iris) Teardown() {
	i.pub.Close()
	i.sub.Close()
}

func (i *Iris) Send(message []byte) {
	i.pub.Publish(i.topic, message)
}

func (i *Iris) MessageHandler() *benchmark.MessageHandler {
	return &i.handler
}
