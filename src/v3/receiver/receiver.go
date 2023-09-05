// Receiver
package main

import (
	"context"
	"encoding/binary"
	"io/ioutil"
	"log"
	"net"
	"net/http"
)

const (
	addr   = ":8080"          // receiver address
	broker = "localhost:9090" // broker address
)

var messages = make(chan []byte, 100) // create a buffered channel for messages with capacity 100

func main() {
	// create a listener for the broker connection
	listener, err := net.Listen("tcp", broker)
	if err != nil {
		log.Fatal("Error listening on broker address:", err)
	}
	defer listener.Close()

	log.Println("Listening on broker address:", broker)

	// accept a single connection from the broker
	conn, err := listener.Accept()
	if err != nil {
		log.Fatal("Error accepting connection from broker:", err)
	}
	defer conn.Close()

	log.Println("Accepted connection from broker:", conn.RemoteAddr())

	// handle the connection in a separate goroutine
	go handle(conn)

	// create a handler for the HTTP endpoint
	handler := func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		select {
		case <-ctx.Done():
			log.Println("HTTP request canceled")
			w.WriteHeader(http.StatusInternalServerError)
			return
		default:
			// read the message from the request body
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Println("Error reading message from request body:", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			r.Body.Close()

			size := len(data)

			log.Println("Received message of size", size)

			select {
			case <-ctx.Done():
				log.Println("HTTP request canceled")
				w.WriteHeader(http.StatusInternalServerError)
				return
			case messages <- data:
				log.Println("Sent message to broker channel")
				w.WriteHeader(http.StatusOK)
			default:
				log.Println("Broker channel full, dropping message") // or use a buffered channel
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		}
	}

	log.Println("Listening on receiver address:", addr)

	http.HandleFunc("/send", handler) // register the handler for the /send path

	log.Fatal(http.ListenAndServe(addr, nil)) // start the HTTP server
}

func handle(conn net.Conn) {
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		// check if the context is done
		select {
		case <-ctx.Done():
			log.Println("Broker connection canceled")
			return
		default:
			// continue with reading the message
		}

		// read the message from the broker channel
		message := <-messages

		size := len(message)

		log.Println("Received message of size", size)

		// check if the context is done
		select {
		case <-ctx.Done():
			log.Println("Broker connection canceled")
			return
		default:
			// continue with writing the message
		}

		if err := binary.Write(conn, binary.BigEndian, uint32(size)); err != nil {
			log.Println("Error writing message size to broker connection:", err)
			return
		}

		if _, err := conn.Write(message); err != nil {
			log.Println("Error writing message to broker connection:", err)
			return
		}

	}
}
