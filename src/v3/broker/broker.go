// Broker
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	receiver = "localhost:9090" // receiver address
	dest     = "localhost:8081" // destination address
)

var pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 8192) // maximum message size in bytes
	},
}

var messages = make(chan []byte) // create a channel for messages

func main() {

	// open a log.json file
	logFile, err := os.OpenFile("log.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	// create a zap logger with custom configuration
	// assign two variables to receive the return values from zap.New()
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), // use JSON encoder
		zapcore.AddSync(logFile),                                 // write to log.json file
		zap.InfoLevel,                                            // set minimum log level to INFO
	))

	defer logger.Sync()

	// add a name to the logger
	logger = logger.Named("broker")

	// create a dialer for the destination connection
	dialer := net.Dialer{}

	//logger.Info("Dialing destination address", zap.String("address", dest))
	fmt.Printf("Dialing destination address: %v", dest)

	conn, err := dialer.Dial("tcp", dest)
	if err != nil {
		//logger.Fatal("Error dialing destination address", zap.Error(err))
		log.Fatalf("Error dialing destination address, %v", err)
	}
	defer conn.Close()

	//logger.Info("Connected to destination address", zap.String("address", dest))
	fmt.Printf("Connected to destination address: %v", dest)

	for {
		//logger.Info("Dialing receiver address", zap.String("address", receiver))
		fmt.Printf("Dialing receiver address: %v", receiver)

		rconn, err := dialer.Dial("tcp", receiver)
		if err != nil {
			//logger.Error("Error dialing receiver address", zap.Error(err))
			fmt.Printf("Error dialing receiver address, %v", err)
			continue
		}

		//logger.Info("Connected to receiver address", zap.String("address", receiver))
		fmt.Printf("Connected to receiver address, %v", receiver)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)

		// create a multi writer that writes to both the file and the standard output
		mw := zapcore.NewMultiWriteSyncer(
			zapcore.AddSync(logFile),
		)

		// handle the destination connection with the multi writer in a separate goroutine
		go func() {
			defer wg.Done()
			handleDestination(ctx, conn, logger, mw)
		}()

		// handle the receiver connection with the multi writer
		handleReceiver(ctx, rconn, logger, mw)

		// wait for the goroutines to finish
		wg.Wait()

		logger.Info("Broker done")

	}
}

func handleReceiver(ctx context.Context, conn net.Conn, logger *zap.Logger, mw zapcore.WriteSyncer) {
	defer conn.Close()

	for {

		select {
		case <-ctx.Done():
			//logger.Info("Receiver connection canceled")
			fmt.Println("Receiver connection canceled")
			return
		default:
			// read the message from the receiver connection
			buf := pool.Get().([]byte)
			defer pool.Put(buf)

			n, err := conn.Read(buf)
			if err != nil {
				//logger.Error("Error reading message from receiver connection", zap.Error(err))
				fmt.Printf("Error reading message from receiver connection, %v", err)
				return
			}

			message := buf[:n]

			//logger.Info("Received message of size", zap.Int("size", n), zap.ByteString("data", message))
			//fmt.Printf("Received message of size :%v \n message: %v", n, message)

			// send the message to the channel without blocking or dropping
			messages <- message

			//logger.Info("Sent message to destination channel")
			fmt.Println("Sent message to destination channel")

			// write the message to the log file and the standard output as JSON object
			if _, err := mw.Write([]byte("{\"message\": \"" + string(message) + "\"}\n")); err != nil {
				//logger.Error("Error writing message to log file and standard output:", zap.Error(err))
				fmt.Printf("Error writing message to log file and standard output: %v", err)
				return
			}

			//logger.Info("Sent message to log channel")
			fmt.Println("Sent message to log channel")
		}

	}
}

func handleDestination(ctx context.Context, conn net.Conn, logger *zap.Logger, mw zapcore.WriteSyncer) {
	defer conn.Close()

	for {

		select {
		case <-ctx.Done():
			//logger.Info("Destination connection canceled")
			fmt.Println("Destination connection canceled")
			return
		case message := <-messages:
			//logger.Info("Received message from destination channel")
			fmt.Println("Received message from destination channel")

			size := len(message)

			if err := binary.Write(conn, binary.BigEndian, uint32(size)); err != nil {
				//logger.Error("Error writing message size to destination connection", zap.Error(err))
				fmt.Printf("Error writing message size to destination connection: %v", err)
				return
			}

			if _, err := conn.Write(message); err != nil {
				//logger.Error("Error writing message to destination connection", zap.Error(err))
				fmt.Printf("Error writing message to destination connection: %v", err)
				return
			}

		}

	}
}
