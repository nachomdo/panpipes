package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	flag "github.com/spf13/pflag"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func openRemoteConnection(addr string) (*bufio.ReadWriter, error) {
	log.Println("Openning connection to " + addr)
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		return nil, fmt.Errorf("Error opening a connection to %s \n", addr)
	}

	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}

func restoreToKafka(config PanpipesConfig) error {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.brokers})
	defer producer.Close()

	var f *os.File
	if config.file == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(config.file)

		if err != nil {
			log.Fatalln("Error opening the file ", err)
		}

		defer f.Close()
	}

	r := bufio.NewReader(f)

	for {
		var data kafka.Message
		decoder := gob.NewDecoder(r)
		err := decoder.Decode(&data)

		if err != nil {
			if err != io.EOF {
				log.Println("Error decoding GOB data: ", err)
				continue
			} else {
				break
			}
		}

		data.TopicPartition = kafka.TopicPartition{Topic: &config.topic, Partition: kafka.PartitionAny}
		producer.Produce(&data, nil)
		log.Println("Received message ", string(data.Value))
	}
	producer.Flush(5000)
	return nil
}

func tcpListenerToKafka(config PanpipesConfig) error {
	listener, err := net.Listen("tcp", ":8888")

	if err != nil {
		return fmt.Errorf("Error listening on interface %s \n", err)
	}

	defer listener.Close()
	log.Println("Listening on " + listener.Addr().String())

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.brokers})
	defer producer.Close()

	for {
		conn, _ := listener.Accept()
		go func(c net.Conn) {
			defer c.Close()
			rw := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
			for {
				var data kafka.Message
				decoder := gob.NewDecoder(rw)
				err := decoder.Decode(&data)

				if err != nil {
					if err != io.EOF {
						log.Println("Error decoding GOB data: ", err)
						continue
					} else {
						break
					}
				}

				data.TopicPartition = kafka.TopicPartition{Topic: &config.topic, Partition: kafka.PartitionAny}
				producer.Produce(&data, nil)
				log.Println("Received message ", string(data.Value))
			}
		}(conn)
	}
}

func kafkaToChannel(config PanpipesConfig, messageForwarder func(chan kafka.Message, *bufio.ReadWriter)) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        config.brokers,
		"group.id":                 time.Now().String(),
		"session.timeout.ms":       "6000",
		"go.events.channel.enable": true,
		"default.topic.config":     kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer %s \n", err)
		os.Exit(1)
	}

	log.Printf("Creating consumer %v \n", c)

	c.SubscribeTopics([]string{config.topic}, nil)
	defer c.Close()

	messagesBuffer := make(chan kafka.Message, 1000)

	var rw *bufio.ReadWriter
	if config.file != "" {
		var f *os.File
		if config.file == "-" {
			f = os.Stdout
		} else {
			f, err = os.Create(config.file)

			if err != nil {
				log.Fatalf("Error creating new file %v \n", err)
			}
			defer f.Close()
		}

		rw = bufio.NewReadWriter(bufio.NewReader(f), bufio.NewWriter(f))
	} else {
		rw, err = openRemoteConnection(config.connect)
	}

	if err != nil {
		log.Fatalf("Error opening remote connection %s", err)
	}

	go messageForwarder(messagesBuffer, rw)

	for {
		ev := <-c.Events()
		switch e := ev.(type) {
		case *kafka.Message:
			log.Printf("Message received %s %s \n", string(e.Value))
			messagesBuffer <- *e

		case kafka.Error:
			fmt.Fprintf(os.Stderr, "Error reading from Kafka %v \n", e)
			break
		}
	}
}

type PanpipesConfig struct {
	connect string
	file    string
	brokers string
	topic   string
	restore string
}

func main() {
	config := PanpipesConfig{}

	flag.StringVar(&config.connect, "connect", "", "IP address to connect to. If not provided it will start in a server mode")
	flag.StringVar(&config.file, "file", "", "File to write or read data from")
	flag.StringVar(&config.brokers, "brokers", "localhost:9092", "A comma separated list of brokers to bootstrap from")
	flag.StringVar(&config.topic, "topic", "", "The topic to read from or produce to")
	flag.StringVar(&config.restore, "restore", "", "Restore mode")

	flag.Parse()

	if config.restore != "" {
		restoreToKafka(config)
		os.Exit(0)
	}

	if config.connect == "" {
		err := tcpListenerToKafka(config)

		if err != nil {
			fmt.Printf("Error starting listener %v \n", err)
		}
	} else {
		kafkaToChannel(config, func(messageBuffer chan kafka.Message, rw *bufio.ReadWriter) {
			for msg := range messageBuffer {
				encoder := gob.NewEncoder(rw)
				err := encoder.Encode(msg)

				if err != nil {
					log.Println("Error encoding kafka.Messsage", err)
				}

				err = rw.Flush()

				if err != nil {
					log.Println("Flush failed", err)
				}

			}
		})
	}
}
