package mq

import (
	"github.com/garyburd/redigo/redis"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Redis struct {
	handler benchmark.MessageHandler
	pub     redis.Conn
	sub     redis.PubSubConn
	channel string
}

func redisReceive(r *Redis) {
	for {
		switch v := r.sub.Receive().(type) {
		case redis.Message:
			if r.handler.ReceiveMessage(v.Data) {
				break
			}
		}
	}
}

func NewRedis(numberOfMessages int, testLatency bool) *Redis {
	channel := "test"
	pub, _ := redis.Dial("tcp", ":6379")
	subConn, _ := redis.Dial("tcp", ":6379")
	sub := redis.PubSubConn{subConn}
	sub.Subscribe(channel)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Redis{
		handler: handler,
		pub:     pub,
		sub:     sub,
		channel: channel,
	}
}

func (r *Redis) Setup() {
	go redisReceive(r)
}

func (r *Redis) Teardown() {
	r.pub.Close()
	r.sub.Close()
}

func (r *Redis) Send(message []byte) {
	r.pub.Send("PUBLISH", r.channel, message)
	r.pub.Flush()
}

func (r *Redis) MessageHandler() *benchmark.MessageHandler {
	return &r.handler
}
