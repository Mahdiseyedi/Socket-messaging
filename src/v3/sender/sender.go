// Message sender producer
package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"
	"net/http"
	"time"
)

const (
	minSize = 50                           // minimum message size in bytes
	maxSize = 8192                         // maximum message size in bytes
	rate    = 10000                        // message rate per second
	url     = "http://localhost:8080/send" // receiver endpoint
)

func main() {
	// create a ticker to send messages at a fixed rate
	ticker := time.NewTicker(time.Second / rate)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// generate a random message with a random size
			size := rand.Intn(maxSize-minSize+1) + minSize
			data := make([]byte, size+4) // add 4 bytes for the size header
			rand.Read(data[4:])          // fill the data with random bytes

			// write the message size to the first 4 bytes of the data
			binary.BigEndian.PutUint32(data[:4], uint32(size))

			log.Println("Generated message of size", size)

			// send the message to the receiver endpoint using HTTP POST
			resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(data))
			if err != nil {
				log.Println("Error sending message:", err)
				continue
			}

			// read and discard the response body
			resp.Body.Close()

			log.Println("Sent message of size", size)
		}
	}
}
