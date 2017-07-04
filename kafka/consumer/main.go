// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"github.com/zdavep/dozer"
	"log"
	"os"
	"os/signal"
	"runtime"
)

// Example message handler function.
func messageHandler(id int, messages chan []byte) {
	for message := range messages {
		log.Printf("%d: Received [ %s ]\n", id, string(message))
	}
}

// Consume messages from a Kafka topic.
func main() {

	// Create a dozer Kafka consumer instance
	dz := dozer.Topic("test").WithProtocol("kafka").Consumer()
	err := dz.Connect("localhost", 9092)
	if err != nil {
		log.Println("Error creating kafka consumer!")
		log.Fatal(err)
	}

	// Helper channels
	messages, quit := make(chan []byte), make(chan bool)

	// Dedicate some go-routines for message processing.
	for i := 1; i <= runtime.NumCPU()/2+1; i++ {
		go messageHandler(i, messages)
	}

	// Start receiving messages
	log.Println("Receiving messages; hit [ctrl-c] to quit.")
	go func() {
		if err := dz.RecvLoop(messages, quit); err != nil {
			log.Fatal(err)
		}
	}()

	// Listen for [ctrl-c] interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	signal.Stop(interrupt)
	quit <- true
	os.Exit(0)
}
