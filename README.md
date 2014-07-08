mq-benchmarking
==========================

Results: http://www.bravenewgeek.com/dissecting-message-queues/

Usage: `go run main.go subject [test_latency] [num_messages] [message_size]`

subject: inproc, zeromq, nanomsg, kestrel, kafka, rabbitmq, nsq, redis, activemq, gnatsd

test_latency: `true` will test latency, `false` will test throughput

num_messages: number of messages to send in the test

message_size: size of each message in bytes
