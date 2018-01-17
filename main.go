package main

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	flag "github.com/spf13/pflag"
	"log"
	"net"
	"os"
	"time"
)

func buildSourceFromConfig(config PanpipesConfig) (*bufio.ReadWriter, error) {
	f := os.Stdin
	if config.file != "-" {
		var err error
		f, err = os.Open(config.file)
		if err != nil {
			return nil, fmt.Errorf("Error opening the file %v \n", err)
		}
	}
	return bufio.NewReadWriter(bufio.NewReader(f), bufio.NewWriter(f)), nil
}

func buildSinkFromConfig(config PanpipesConfig) (*bufio.ReadWriter, error) {
	if config.file != "" {
		f := os.Stdout
		if config.file != "-" {
			var err error
			f, err = os.Create(config.file)
			if err != nil {
				return nil, fmt.Errorf("Error creating new file %v \n", err)
			}
		}
		return bufio.NewReadWriter(bufio.NewReader(f), bufio.NewWriter(f)), nil
	} else {
		log.Println("Opening connection to " + config.connect)
		conn, err := net.Dial("tcp", config.connect)

		if err != nil {
			return nil, fmt.Errorf("Error opening a connection to %s \n", config.connect)
		}

		return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
	}
}

func buildConsumerFromConfig(config PanpipesConfig) (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        config.brokers,
		"group.id":                 time.Now().String(),
		"session.timeout.ms":       "6000",
		"go.events.channel.enable": true,
		"default.topic.config":     kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})

	if err != nil {
		return nil, fmt.Errorf("Failed to create consumer %s \n", err)
	}

	log.Printf("Creating consumer %v \n", c)

	c.SubscribeTopics([]string{config.topic}, nil)
	return c, nil
}

func parseDateFlags(s string, fallback time.Time) (time.Time, error) {
	const layout = "2-1-2006"

	if s != "" {
		result, err := time.Parse(layout, s)
		if err != nil {
			return fallback, err
		}
		return result, nil
	}
	return fallback, nil
}

func buildPredicatesFromConfig(config PanpipesConfig) []KafkaPredicate {
	result := []KafkaPredicate{&NoOpPredicate{}}
	if config.count > 0 {
		result[0] = &MessageCounter{MaxCount: config.count}
	}

	if config.from != "" || config.until != "" {
		min, err := parseDateFlags(config.from, time.Time{})
		if err != nil {
			log.Fatalf("Error parsing starting date %s: %v", config.from, err)
		}
		max, err := parseDateFlags(config.until, time.Now())
		if err != nil {
			log.Fatalf("Error parsing end date %s: %v", config.until, err)
		}
		log.Println("End ", max, min)
		result[0] = &TimestampRestriction{MinDate: min, MaxDate: max}
	}

	return result
}

type PanpipesConfig struct {
	connect string
	file    string
	brokers string
	topic   string
	listen  string
	from    string
	until   string
	count   int
	restore bool
}

func main() {
	config := PanpipesConfig{}

	flag.StringVarP(&config.listen, "listen", "l", "", "Network interface and port to listen")
	flag.StringVarP(&config.file, "file", "f", "", "File to write or read data from")
	flag.StringVarP(&config.brokers, "brokers", "b", "localhost:9092", "A comma separated list of brokers to bootstrap from")
	flag.StringVarP(&config.topic, "topic", "t", "", "The topic to read from or produce to")
	flag.StringVarP(&config.from, "since", "s", "", "Filter messages with a timestamp before this date")
	flag.StringVarP(&config.until, "until", "u", "", "Filter messages with a timestamp after this date")
	flag.BoolVarP(&config.restore, "restore", "r", false, "Indicates that we want to restore a topic from a file")
	flag.IntVarP(&config.count, "count", "c", 0, "Number of messages to consume or restore")
	flag.Parse()

	if config.listen != "" || config.restore {
		producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.brokers})
		defer producer.Close()

		if err != nil {
			log.Fatalf("Cannot create Kafka producer %v", err)
		}

		if config.listen != "" {
			TCPListenerToKafka(config, producer.ProduceChannel())
		} else {
			rw, err := buildSourceFromConfig(config)

			if err != nil {
				log.Fatalf("Cannot open the provided source %v", err)
			}

			GobSourceToKafkaChannel(config.topic, rw.Reader, producer.ProduceChannel())
			producer.Flush(5000)
		}
	} else {
		config.connect = os.Args[len(os.Args)-1]
		c, err := buildConsumerFromConfig(config)
		defer c.Close()

		if err != nil {
			log.Fatal(err)
		}

		rw, err := buildSinkFromConfig(config)

		if err != nil {
			log.Fatal(err)
		}

		KafkaChannelToGobSink(c.Events(), rw, buildPredicatesFromConfig(config))
	}
}
