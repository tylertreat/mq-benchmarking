package benchmarking

import "fmt"
import "time"

type MessageSender interface {
    Send([]byte)
}

type MessageSendingMachine struct {
    Sender MessageSender
    ReportingInterval int
}

func (s MessageSendingMachine) Run(messageSize int, numberToSend int) {
    message := make([]byte, messageSize)
    start := time.Now().UnixNano()

    for i := 0; i < numberToSend; i++ {
        if i % s.ReportingInterval == 0 {
            fmt.Println("Sent message %d", i)
        }

        s.Sender.Send(message)
    }

    stop := time.Now().UnixNano()
    elapsedMs := float32(stop - start) / 1000000.0
    fmt.Println(elapsedMs)
}

