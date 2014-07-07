package benchmark

import (
	"encoding/binary"
	"log"
	"time"
)

type MessageReceiver interface {
	MessageHandler() *MessageHandler
	Setup()
	Teardown()
}

type ReceiveEndpoint struct {
	MessageReceiver  MessageReceiver
	NumberOfMessages int
	Handler          *MessageHandler
}

func NewReceiveEndpoint(receiver MessageReceiver, numberOfMessages int) *ReceiveEndpoint {
	return &ReceiveEndpoint{
		MessageReceiver:  receiver,
		NumberOfMessages: numberOfMessages,
		Handler:          receiver.MessageHandler(),
	}
}

type MessageHandler interface {
	ReceiveMessage([]byte) bool
	HasCompleted() bool
}

type ThroughputMessageHandler struct {
	hasStarted       bool
	hasCompleted     bool
	messageCounter   int
	NumberOfMessages int
	started          int64
	stopped          int64
}

type LatencyMessageHandler struct {
	NumberOfMessages int
	Latencies        []float32
	messageCounter   int
	hasCompleted     bool
}

func (handler *ThroughputMessageHandler) HasCompleted() bool {
	return handler.hasCompleted
}

func (handler *ThroughputMessageHandler) ReceiveMessage(message []byte) bool {
	if !handler.hasStarted {
		handler.hasStarted = true
		handler.started = time.Now().UnixNano()
	}

	handler.messageCounter++

	if handler.messageCounter == handler.NumberOfMessages {
		handler.stopped = time.Now().UnixNano()
		handler.hasCompleted = true
		ms := float32(handler.stopped-handler.started) / 1000000.0
		log.Printf("Received %d messages in %f ms\n", handler.NumberOfMessages, ms)
		log.Printf("Received %f per second\n", 1000*float32(handler.NumberOfMessages)/ms)
		return true
	}

	return false
}

func (handler *LatencyMessageHandler) HasCompleted() bool {
	return handler.hasCompleted
}

func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	now := time.Now().UnixNano()
	then, _ := binary.Varint(message)

	//if then != 0 {
	handler.Latencies = append(handler.Latencies, (float32(now-then))/1000/1000)
	//}

	handler.messageCounter++
	if handler.messageCounter == handler.NumberOfMessages {
		handler.hasCompleted = true
		sum := float32(0)
		for _, latency := range handler.Latencies {
			sum += latency
		}
		avgLatency := float32(sum) / float32(len(handler.Latencies))
		log.Printf("Mean latency for %d messages: %f ms\n", handler.NumberOfMessages,
			avgLatency)
		return true
	}

	return false
}

func (endpoint ReceiveEndpoint) WaitForCompletion() {
	for {
		if (*endpoint.Handler).HasCompleted() {
			break
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
