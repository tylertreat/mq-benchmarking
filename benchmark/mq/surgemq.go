package mq

import (
	"github.com/surge/surgemq/auth"
	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/service"
	"github.com/surge/surgemq/sessions"
	"github.com/surge/surgemq/topics"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type SurgeMQ struct {
	handler     benchmark.MessageHandler
	client      *service.Client
	subject     string
	testLatency bool
}

func NewSurgeMQ(numberOfMessages int, testLatency bool) *SurgeMQ {
	uri := "tcp://127.0.0.1:1883"

	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte("surgemq"))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte("surgemq"))
	msg.SetPassword([]byte("verysecret"))

	c, err := service.Connect(service.Context{
		KeepAlive:      service.DefaultKeepAlive,
		ConnectTimeout: service.DefaultConnectTimeout,
		AckTimeout:     service.DefaultAckTimeout,
		TimeoutRetries: service.DefaultTimeoutRetries,
		Auth:           auth.MockSuccessAuthenticator,
		Topics:         topics.NewMemTopics(),
		Store:          sessions.NewMemStore(),
	}, uri, msg)

	if err != nil {
		panic(err)
	}

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &SurgeMQ{
		handler:     handler,
		subject:     "test",
		client:      c,
		testLatency: testLatency,
	}
}

func (smq *SurgeMQ) Setup() {
	msg := message.NewSubscribeMessage()
	msg.SetPacketId(1)
	msg.AddTopic([]byte(smq.subject), 0)

	smq.client.Subscribe(msg, nil, func(msg *message.PublishMessage) error {
		smq.handler.ReceiveMessage(msg.Payload())
		return nil
	})
}

func (smq *SurgeMQ) Teardown() {
	smq.client.Disconnect()
}

func (smq *SurgeMQ) Send(msgbytes []byte) {
	msg := message.NewPublishMessage()
	msg.SetTopic([]byte(smq.subject))
	msg.SetPayload(msgbytes)
	msg.SetQoS(0)

	smq.client.Publish(msg, nil)
}

func (smq *SurgeMQ) MessageHandler() *benchmark.MessageHandler {
	return &smq.handler
}
