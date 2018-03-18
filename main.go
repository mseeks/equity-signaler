package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/go-redis/redis"
)

var (
	broker        string
	consumer      *cluster.Consumer
	consumerGroup string
	consumerTopic string
	producer      sarama.AsyncProducer
	producerTopic string
)

type message struct {
	Signal string `json:"signal"`
	At     string `json:"at"`
}

type statsMessage struct {
	Macd              string `json:"macd"`
	MacdSignal        string `json:"macd_signal"`
	MacdDecayedSignal string `json:"macd_decayed_signal"`
	At                string `json:"at"`
}

func hasChanged(symbol string, signal string) (bool, error) {
	// Create the Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ENDPOINT"),
		Password: "",
		DB:       0,
	})
	defer redisClient.Close()

	if os.Getenv("TEST_MODE") != "true" {
		// Short circuit if it's recently changed, this helps prevent duplicate signals
		recentlyChanged, err := redisClient.Get(fmt.Sprint(symbol, "_recently_changed")).Result()
		if err == redis.Nil {
			recentlyChanged = "false"
		} else if err != nil {
			return false, err
		}

		// If the signal recently changed, then skip the rest of the process until entry expires
		if recentlyChanged == "true" {
			fmt.Println("Short-circuit signaling process, signal was recently changed.")
			return false, nil
		}
	}

	// Check if there's an existing value in Redis
	existingValue, err := redisClient.Get(fmt.Sprint(symbol, "_signal")).Result()
	if err == redis.Nil {
		// Set the value if it didn't exist already
		setErr := redisClient.Set(fmt.Sprint(symbol, "_signal"), signal, 0).Err()
		if setErr != nil {
			panic(setErr)
		}
	} else if err != nil {
		panic(err)
	} else {
		if existingValue != signal {
			// Set to the new value
			err := redisClient.Set(fmt.Sprint(symbol, "_signal"), signal, 0).Err()
			if err != nil {
				panic(err)
			}

			// Set recently changed to twelve hours to allow a timeout so duplicate signals are reduced
			// Twelve hours should make sure that we aren't producing more than one signal for any given trading day
			err = redisClient.Set(fmt.Sprint(symbol, "_recently_changed"), "true", 12*time.Hour).Err()
			if err != nil {
				panic(err)
			}

			return true, nil
		}
	}

	return false, nil
}

func broadcastSignal(symbol string, signal string, at string) {
	signalMessage := message{
		Signal: signal,
		At:     at,
	}

	jsonMessage, err := json.Marshal(signalMessage)
	if err != nil {
		fmt.Println(err)
		return
	}

	jsonMessageString := string(jsonMessage)
	fmt.Println("Sending:", symbol, "->", jsonMessageString)

	producer.Input() <- &sarama.ProducerMessage{
		Topic: producerTopic,
		Key:   sarama.StringEncoder(symbol),
		Value: sarama.StringEncoder(jsonMessage),
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
		broadcastSignal(symbol, signal, stats.At)
	}
}

// Entrypoint for the program
func main() {
	broker = os.Getenv("KAFKA_ENDPOINT")
	consumerTopic = os.Getenv("KAFKA_CONSUMER_TOPIC")
	consumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP")
	producerTopic = os.Getenv("KAFKA_PRODUCER_TOPIC")

	// init config
	consumerConfig := cluster.NewConfig()
	producerConfig := sarama.NewConfig()

	producerConfig.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	producerConfig.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	producerConfig.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	// init consumer
	brokers := []string{broker}
	topics := []string{consumerTopic}

	for {
		var err error

		consumer, err = cluster.NewConsumer(brokers, consumerGroup, topics, consumerConfig)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer consumer.Close()

		producer, err = sarama.NewAsyncProducer(brokers, producerConfig)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer producer.Close()

		go func() {
			for err := range consumer.Errors() {
				log.Println("Error:", err)
			}
		}()

		go func() {
			for err := range producer.Errors() {
				log.Println("Error:", err)
			}
		}()

		// consume messages
		for {
			select {
			case msg, ok := <-consumer.Messages():
				if ok {
					symbol := string(msg.Key)
					stats := statsMessage{}

					fmt.Println("Received:", symbol, "->", string(msg.Value))

					err = json.Unmarshal(msg.Value, &stats)
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

					if sentAt.Unix() < anHourAgo && os.Getenv("TEST_MODE") != "true" {
						fmt.Println("Message has expired, ignoring.")

						continue
					}

					signalEquity(symbol, stats)

					consumer.MarkOffset(msg, "") // mark message as processed
				}
			}
		}
	}
}
