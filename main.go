package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/antonholmquist/jason"
	"github.com/go-redis/redis"
	"github.com/jasonlvhit/gocron"
	"github.com/segmentio/kafka-go"
	"gopkg.in/resty.v1"
)

// Used to represent an equity that we're watching for signal changes
type equity struct {
	symbol string
}

type message struct {
	Signal string `json:"signal"`
	At     string `json:"at"`
}

type byDate []time.Time

func (s byDate) Len() int {
	return len(s)
}
func (s byDate) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byDate) Less(i, j int) bool {
	return s[i].Unix() < s[j].Unix()
}

func (equity *equity) query() ([]byte, error) {
	resp, err := resty.R().
		SetQueryParams(map[string]string{
			"function":    "MACD",
			"symbol":      equity.symbol,
			"interval":    "daily",
			"series_type": "close",
			"apikey":      os.Getenv("ALPHAVANTAGE_API_KEY"),
		}).
		SetHeader("Accept", "application/json").
		Get("https://www.alphavantage.co/query")
	if err != nil {
		return []byte(""), err
	}

	if resp.StatusCode() != 200 {
		return []byte(""), fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	return resp.Body(), nil
}

// Returns a BUY or SELL signal for the equity
func (equity *equity) signal() (string, error) {
	query, err := equity.query()
	if err != nil {
		return "", err
	}

	value, err := jason.NewObjectFromBytes(query)
	if err != nil {
		return "", err
	}

	technicalAnalysis, err := value.GetObject("Technical Analysis: MACD")
	if err != nil {
		if strings.Contains(err.Error(), "Please consider optimizing your API call frequency.") {
			return "", fmt.Errorf("External API has enforced rate limiting")
		}
		return "", err
	}

	var days []time.Time
	dateLayout := "2006-01-02"
	dateAndTimeLayout := "2006-01-02 15:04:05"

	for key := range technicalAnalysis.Map() {
		day, e := time.Parse(dateLayout, key)
		if e != nil {
			day, e = time.Parse(dateAndTimeLayout, key)
			if e != nil {
				return "", err
			}
		}

		days = append(days, day)
	}

	sort.Sort(byDate(days))

	lastKey := days[len(days)-1]
	keyShort := lastKey.Format(dateLayout)
	keyLong := lastKey.Format(dateAndTimeLayout)

	macdString, err := value.GetString("Technical Analysis: MACD", keyShort, "MACD")
	if err != nil {
		macdString, err = value.GetString("Technical Analysis: MACD", keyLong, "MACD")
		if err != nil {
			return "", err
		}
	}

	macdSignalString, err := value.GetString("Technical Analysis: MACD", keyShort, "MACD_Signal")
	if err != nil {
		macdSignalString, err = value.GetString("Technical Analysis: MACD", keyLong, "MACD_Signal")
		if err != nil {
			return "", err
		}
	}

	macd, err := strconv.ParseFloat(macdString, 64)
	if err != nil {
		return "", err
	}

	macdSignal, err := strconv.ParseFloat(macdSignalString, 64)
	if err != nil {
		return "", err
	}

	var signal string

	if macd > macdSignal {
		signal = "BUY"
	} else {
		signal = "SELL"
	}

	return signal, nil
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

func (equity *equity) broadcastSignal(signal string) {
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{os.Getenv("KAFKA_ENDPOINT")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		Balancer: &kafka.RoundRobin{},
	})
	defer producer.Close()

	signalMessage := message{
		Signal: signal,
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

func watchEquity(symbol string) {
	go func() {
		watchedEquity := equity{strings.ToUpper(symbol)}

		// Fetch the signal for the equity
		signal, err := watchedEquity.signal()
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		hasChanged, err := watchedEquity.hasChanged(signal)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		if hasChanged {
			watchedEquity.broadcastSignal(signal)
		}
	}()
}

// Shuffles an array in place
func shuffle(vals []string) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for len(vals) > 0 {
		n := len(vals)
		randIndex := r.Intn(n)
		vals[n-1], vals[randIndex] = vals[randIndex], vals[n-1]
		vals = vals[:n-1]
	}
}

// Entrypoint for the program
func main() {
	// Initialize a new scheduler
	scheduler := gocron.NewScheduler()

	// Get a list of equities from the environment variable
	equityEatchlist := strings.Split(os.Getenv("EQUITY_WATCHLIST"), ",")

	// Shuffle watchlist so ENV order of equities isn't a weighted factor and it's more dependent on time-based priority
	// This should only really matter if there's a case where to equities change direction during the same interval (unlikely)
	shuffle(equityEatchlist)

	// For each equity in the watchlist schedule it to be watched every 15 minutes
	for _, equitySymbol := range equityEatchlist {
		time.Sleep(5 * time.Second)
		scheduler.Every(15).Minutes().Do(watchEquity, equitySymbol)
		watchEquity(equitySymbol) // Watch the signal immediately rather than wait until next trigger
	}

	// Start the scheduler process
	<-scheduler.Start()
}
