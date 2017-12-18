package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis"
)

// Used to represent an equity that we're watching for signal changes
type equity struct {
	macd       float64
	macdSignal float64
	symbol     string
	signal     string
}

type message struct {
	Signal string `json:"signal"`
	At     string `json:"at"`
}

// Returns a BUY or SELL signal for the equity
func newEquity(symbol string, macd string, macdSignal string) (equity, error) {
	macdFloat, err := strconv.ParseFloat(macd, 64)
	if err != nil {
		return equity{}, err
	}

	macdSignalFloat, err := strconv.ParseFloat(macdSignal, 64)
	if err != nil {
		return equity{}, err
	}

	equity := equity{
		macd:       macdFloat,
		macdSignal: macdSignalFloat,
		signal:     "",
		symbol:     symbol,
	}

	return equity, nil
}

// Returns a BUY or SELL signal for the equity
func (equity *equity) setSignal() {
	if equity.macd > equity.macdSignal {
		equity.signal = "BUY"
	} else {
		equity.signal = "SELL"
	}
}

func (equity *equity) hasChanged(signal string) (bool, error) {
	// Create the Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ENDPOINT"),
		Password: "",
		DB:       0,
	})
	defer client.Close()

	// Check if there's an existing value in Redis
	existingValue, err := client.Get(equity.symbol).Result()
	if err == redis.Nil {
		// Set the value if it didn't exist already
		setErr := client.Set(equity.symbol, signal, 0).Err()
		if setErr != nil {
			panic(setErr)
		}
	} else if err != nil {
		panic(err)
	} else {
		// If the signal has changed direction
		if existingValue != signal {
			// Set to the new value
			err := client.Set(equity.symbol, signal, 0).Err()
			if err != nil {
				panic(err)
			}

			return true, nil
		}
	}

	return false, nil
}

func (equity *equity) broadcastSignal() {
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{os.Getenv("KAFKA_ENDPOINT")},
		Topic:    os.Getenv("KAFKA_SIGNALS_TOPIC"),
		Balancer: &kafka.LeastBytes{},
	})
	defer producer.Close()

	signalMessage := message{
		Signal: equity.signal,
		At:     time.Now().UTC().Format("2006-01-02 15:04:05 -0700"),
	}

	jsonMessage, err := json.Marshal(signalMessage)
	if err != nil {
		panic(err)
	}

	producer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(equity.symbol),
			Value: jsonMessage,
		},
	)

	jsonMessageString := string(jsonMessage)
	fmt.Println(equity.symbol, "->", jsonMessageString)
}

// Entrypoint for the program
func main() {
	watchedEquity, err := newEquity(symbol, macd, macdSignal)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Set the signal for the equity based on stats
	watchedEquity.setSignal()

	hasChanged, err := watchedEquity.hasChanged()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if hasChanged {
		watchedEquity.broadcastSignal()
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  os.Getenv("KAFKA_ENDPOINT"),
		"group.id":           os.Getenv("KAFKA_GROUP_ID"),
		"session.timeout.ms": 6000,
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset":       "latest",
			"auto.commit.interval.ms": 1000,
		}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(os.Getenv("KAFKA_MACD_TOPIC"), nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n%s\n",
					e.TopicPartition, string(e.Value), string(e.Key))
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
