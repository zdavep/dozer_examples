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
	"sync"
)

// Used to wait until workers have finished
var wg sync.WaitGroup

// Example message handler function.
func messageHandler(id int, messages chan []byte, quit chan bool) {
	defer wg.Done()
	for {
		select {
		case message := <-messages:
			log.Printf("%d: Received [ %s ]\n", id, string(message))
		case <-quit:
			log.Printf("Quit signal received in worker %d\n", id)
			return
		}
	}
}

// Consume messages from a test queue for 10 seconds.
func main() {

	// Create a stomp dozer instance for a queue named "test"
	dz := dozer.Queue("test").WithProtocol("stomp")
	err := dz.Connect("localhost", 61613)
	if err != nil {
		log.Fatal(err)
	}

	// Helper channels
	messages, quit := make(chan []byte), make(chan bool)

	// Dedicate a majority of CPUs to message processing.
	workers := runtime.NumCPU()/2 + 1
	wg.Add(workers)
	for i := 1; i <= workers; i++ {
		go messageHandler(i, messages, quit)
	}

	// Listen for [ctrl-c] interrupt signal
	interrupted, interrupt := make(chan bool), make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		signal.Stop(interrupt)
		interrupted <- true
	}()

	// Start receiving messages
	if err := dz.RecvLoop(messages, interrupted); err != nil {
		log.Println(err)
	}

	// Shut down workers
	for i := 1; i <= workers; i++ {
		log.Printf("Sending quit signal %d\n", i)
		quit <- true
	}

	// Cleanup
	close(messages)
	close(interrupted)
	close(quit)

	// Wait until all workers have completed
	wg.Wait()
}
