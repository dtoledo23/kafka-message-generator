package main

import (
	"log"
	"os"
	"os/signal"
	"sync"

	"time"

	"encoding/json"

	"github.com/Shopify/sarama"
)

func generateRandomLog(sessionID string, stage int) []byte {
	log := &Log{
		Stage:     stage,
		Timestamp: time.Now().Unix(),
		Message:   RandStringBytesMaskImpr(15),
		SessionID: sessionID,
	}

	jsonLog, err := json.Marshal(log)
	if err != nil {
		return nil
	}

	return jsonLog
}

func sendLog(channel chan<- *sarama.ProducerMessage, log []byte) {
	channel <- &sarama.ProducerMessage{Topic: "test", Value: sarama.ByteEncoder(log)}
}

func mockUser(channel chan<- *sarama.ProducerMessage, kill chan os.Signal) {
	sessionID := RandStringBytesMaskImpr(10)

	for {
		select {

		default:
			sendLog(channel, generateRandomLog(sessionID, 1))
			sendLog(channel, generateRandomLog(sessionID, 2))
			sendLog(channel, generateRandomLog(sessionID, 2))
			sendLog(channel, generateRandomLog(sessionID, 3))
		}
	}
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                sync.WaitGroup
		successes, errors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

	mockUser(producer.Input(), signals)
ProducerLoop:
	for {

		select {
		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop

		default:
			if successes > 100 {
				producer.AsyncClose() // Trigger a shutdown of the producer.
				break ProducerLoop
			}
		}
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}
