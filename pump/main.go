// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"github.com/zdavep/dozer"
	_ "github.com/zdavep/dozer/proto/mangos"
	_ "github.com/zdavep/dozer/proto/stomp"
	"log"
	"os"
	"os/signal"
)

// Consume messages from a test queue and forward them to a "mangos" socket.
func main() {

	// Create a dozer queue instance
	var err error
	queue := dozer.Init("stomp").Consumer("test")
	err = queue.Dial("localhost", 61613)
	if err != nil {
		log.Fatal(err)
	}

	// Create a dozer "mangos" socket instance
	socket := dozer.Init("mangos").Producer("")
	err = socket.Dial("*", 5555) // Bind to all interfaces
	if err != nil {
		log.Fatal(err)
	}

	// Helper channels
	pipe, quit := make(chan []byte), make(chan bool)

	// Listen for [ctrl-c] interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		signal.Stop(interrupt)
		quit <- true
		os.Exit(0)
	}()

	// Start forwarding messages to socket
	go func() {
		if err := socket.SendLoop(pipe, quit); err != nil {
			log.Fatal(err)
		}
	}()

	// Start receiving messages from queue
	log.Println("Pump started; enter [ctrl-c] to quit.")
	if err := queue.RecvLoop(pipe, quit); err != nil {
		log.Fatal(err)
	}
}
