package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
)

type message struct {
	Signal string `json:"signal"`
	At     string `json:"at"`
}

type statsMessage struct {
	Macd       string `json:"macd"`
	MacdSignal string `json:"macd_signal"`
	At         string `json:"at"`
}

func hasChanged(symbol string, signal string) (bool, error) {
	// Create the Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ENDPOINT"),
		Password: "",
		DB:       0,
	})
	defer client.Close()

	// Short circuit if it's recently changed, this helps prevent duplicate signals
	recentlyChanged, err := client.Get(fmt.Sprint(symbol, "_recently_changed")).Result()
	if err == redis.Nil {
		recentlyChanged = "false"
	} else if err != nil {
		panic(err)
	}

	// If the signal recently changed, then skip the rest of the process until entry expires
	if recentlyChanged == "true" {
		fmt.Println("Short-circuit signaling process, signal was recently changed.")
		return false, nil
	}

	// Check if there's an existing value in Redis
	existingValue, err := client.Get(symbol).Result()
	if err == redis.Nil {
		// Set the value if it didn't exist already
		setErr := client.Set(symbol, signal, 0).Err()
		if setErr != nil {
			panic(setErr)
		}
	} else if err != nil {
		panic(err)
	} else {
		if existingValue != signal {
			// Set to the new value
			err := client.Set(symbol, signal, 0).Err()
			if err != nil {
				panic(err)
			}

			// Set recently changed to twelve hours to allow a timeout so duplicate signals are reduced
			// Twelve hours should make sure that we aren't producing more than one signal for any given trading day
			err = client.Set(fmt.Sprint(symbol, "_recently_changed"), "true", 12*time.Hour).Err()
			if err != nil {
				panic(err)
			}

			return true, nil
		}
	}

	return false, nil
}

func broadcastSignal(symbol string, signal string) {
	broker := os.Getenv("KAFKA_ENDPOINT")
	topic := os.Getenv("KAFKA_PRODUCER_TOPIC")

	producer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer producer.Close()

	signalMessage := message{
		Signal: signal,
		At:     time.Now().UTC().Format("2006-01-02 15:04:05 -0700"),
	}

	jsonMessage, err := json.Marshal(signalMessage)
	if err != nil {
		fmt.Println(err)
		return
	}

	jsonMessageString := string(jsonMessage)
	fmt.Println("Sending:", symbol, "->", jsonMessageString)

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(jsonMessage), Key: sarama.StringEncoder(symbol)}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func signalEquity(symbol string, stats statsMessage) {
	var signal string

	macd, err := strconv.ParseFloat(stats.Macd, 64)
	if err != nil {
		fmt.Println(err)
		return
	}

	macdSignal, err := strconv.ParseFloat(stats.MacdSignal, 64)
	if err != nil {
		fmt.Println(err)
		return
	}

	if macd > macdSignal {
		signal = "BUY"
	} else {
		signal = "SELL"
	}

	hasChanged, err := hasChanged(symbol, signal)
	if err != nil {
		fmt.Println(err)
		return
	}

	if hasChanged {
		broadcastSignal(symbol, signal)
	}
}

// Entrypoint for the program
func main() {
	broker := os.Getenv("KAFKA_ENDPOINT")
	topic := os.Getenv("KAFKA_CONSUMER_TOPIC")

	for {
		consumer, err := sarama.NewConsumer([]string{broker}, nil)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer partitionConsumer.Close()

		for {
			select {
			case message := <-partitionConsumer.Messages():
				symbol := string(message.Key)
				stats := statsMessage{}

				fmt.Println("Received:", symbol, "->", string(message.Value))

				err = json.Unmarshal(message.Value, &stats)
				if err != nil {
					fmt.Println(err)
					continue
				}

				sentAt, err := time.Parse("2006-01-02 15:04:05 -0700", stats.At)
				if err != nil {
					fmt.Println(err)
					continue
				}

				anHourAgo := time.Now().UTC().Add(-1 * time.Hour).Unix()

				if sentAt.Unix() < anHourAgo {
					fmt.Println("Message has expired, ignoring.")

					continue
				}

				signalEquity(symbol, stats)
			}
		}
	}
}
