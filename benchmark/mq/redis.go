package mq

import (
	"github.com/garyburd/redigo/redis"
	"github.com/tylertreat/mq-benchmarking/benchmark"
)

type Redis struct {
	handler *benchmark.MessageHandler
	pub     redis.Conn
	sub     redis.PubSubConn
	channel string
}

func redisReceive(r Redis) {
	for {
		switch v := r.sub.Receive().(type) {
		case redis.Message:
			r.ReceiveMessage(v.Data)
		}
	}
}

func NewRedis(numberOfMessages int) Redis {
	channel := "test"
	pub, _ := redis.Dial("tcp", ":6379")
	subConn, _ := redis.Dial("tcp", ":6379")
	sub := redis.PubSubConn{subConn}
	sub.Subscribe(channel)

	return Redis{
		handler: &benchmark.MessageHandler{NumberOfMessages: numberOfMessages},
		pub:     pub,
		sub:     sub,
		channel: channel,
	}
}

func (r Redis) Setup() {
	go redisReceive(r)
}

func (r Redis) Teardown() {
	r.pub.Close()
	r.sub.Close()
}

func (r Redis) Send(message []byte) {
	r.pub.Send("PUBLISH", r.channel, message)
	r.pub.Flush()
}

func (r Redis) ReceiveMessage(message []byte) bool {
	return r.handler.ReceiveMessage(message)
}

func (r Redis) MessageHandler() *benchmark.MessageHandler {
	return r.handler
}
