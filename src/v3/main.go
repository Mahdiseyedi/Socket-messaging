package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
	// make interrupt channel and listen for the os signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer close(interrupt)

	start := time.Now()

	//TODO - complete this
	// start all the services
	//go Destination.Destination()
	//go Broker.Broker()
	//go Receiver.Receiver()
	//go Sender.Sender()

	select {
	// wait for the interrupt signal, then print the stats and exit
	case <-interrupt:
		fmt.Printf(
			"total time is: %s",
			time.Since(start).Seconds())
		return
	}
}
