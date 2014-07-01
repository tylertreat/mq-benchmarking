package benchmarking

import "fmt"

type Inproc struct {
    receiver Receiver
}

func (inproc Inproc) Send(message []byte) {
    fmt.Println(inproc.receiver)
    inproc.receiver(message)
}

func (inproc Inproc) Receive(receiver Receiver) {
    inproc.receiver = receiver
}

