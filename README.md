mq-benchmarking
==========================

**Deprecated:** Use [Flotilla](https://github.com/tylertreat/Flotilla) for running benchmarks. See related [blog post](http://www.bravenewgeek.com/benchmark-responsibly/) for more information.

Results: http://www.bravenewgeek.com/dissecting-message-queues/
___

**Usage:** `go run main.go subject [test_latency] [num_messages] [message_size]`

**subject:** inproc, zeromq, nanomsg, kestrel, kafka, rabbitmq, nsq, redis, activemq, nats, beanstalkd, iris

**test_latency:** `true` will test latency, `false` will test throughput

**num_messages:** number of messages to send in the test

**message_size:** size of each message in bytes
