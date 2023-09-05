// Destination
package main

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"sync/atomic"
)

const (
	addr = "localhost:8081" // destination address
)

var count, total int64 // keep track of the number and size of messages

func main() {
	// create a listener for the destination connection
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Error listening on destination address:", err)
	}
	defer listener.Close()

	log.Println("Listening on destination address:", addr)

	for {
		// accept a connection from the broker
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection from broker:", err)
			continue
		}

		log.Println("Accepted connection from broker:", conn.RemoteAddr())

		go handle(conn) // handle the connection in a separate goroutine
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			log.Println("Destination connection canceled")
			return
		default:
			// read the message size from the broker connection
			var size uint32
			if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
				log.Println("Error reading message size from broker connection:", err)
				return
			}

			log.Println("Received message size", size)

			// read the message data from the broker connection
			data := make([]byte, size)
			if _, err := conn.Read(data); err != nil {
				log.Println("Error reading message data from broker connection:", err)
				return
			}

			//log.Println("Received message data", data)

			atomic.AddInt64(&count, 1)           // increment the message count
			atomic.AddInt64(&total, int64(size)) // increment the total size

			log.Println("Processed", atomic.LoadInt64(&count), "messages with total size", atomic.LoadInt64(&total)) // print the processing result
		}
	}
}
