// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"github.com/zdavep/dozer"
	_ "github.com/zdavep/dozer/proto/nats"
	"log"
	"os"
	"os/signal"
	"runtime"
)

// Consume messages from a Kafka topic.
func main() {

	// Create a dozer Kafka consumer instance
	dz := dozer.Init("nats").Consumer("test").WithCredentials("guest", "guest")
	err := dz.Dial("localhost", 4222)
	if err != nil {
		log.Fatal(err)
	}

	// Helper channels
	messages, quit := make(chan []byte), make(chan bool)

	// Dedicate some go-routines for message processing.
	for i := 1; i <= runtime.NumCPU()/2+1; i++ {
		go func(id int, msgs chan []byte) {
			for msg := range msgs {
				log.Printf("%d: Received [ %s ]\n", id, string(msg))
			}
		}(i, messages)
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
