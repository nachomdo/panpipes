package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TCPListenerToKafka(config PanpipesConfig, producerChannel chan<- *kafka.Message) error {
	listener, err := net.Listen("tcp", config.listen)

	if err != nil {
		return fmt.Errorf("Error listening on interface %s \n", err)
	}

	defer listener.Close()
	log.Println("Listening on " + listener.Addr().String())

	for {
		conn, _ := listener.Accept()
		go func(c net.Conn) {
			defer c.Close()
			GobSourceToKafkaChannel(config.topic, bufio.NewReader(c), producerChannel)
		}(conn)
	}
}

func GobSourceToKafkaChannel(topic string, r *bufio.Reader, producerChannel chan<- *kafka.Message) {
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

		if topic != *data.TopicPartition.Topic {
			data.TopicPartition = kafka.TopicPartition{
				Topic: &topic, Partition: kafka.PartitionAny}
		}

		producerChannel <- &data
	}
}

func displayProgresInfo(info <-chan kafka.TopicPartition) {
	var metadata kafka.TopicPartition
	ticker := time.Tick(time.Second * 3)

loop:
	for {
		select {
		case m, ok := <-info:
			metadata = m
			if !ok {
				break loop
			}
		case <-ticker:
			log.Printf("Topic %v partition %v offset %v", *metadata.Topic, metadata.Partition, metadata.Offset)
		}
	}
}

func KafkaChannelToGobSink(events <-chan kafka.Event, rw *bufio.ReadWriter, predicates []KafkaPredicate) {
	progress := make(chan kafka.TopicPartition, 1)
	defer close(progress)
	go displayProgresInfo(progress)

	for msg := range events {
		switch e := msg.(type) {
		case *kafka.Message:
			progress <- e.TopicPartition
			if valid, abort := AllValid(e, predicates); valid {
				encoder := gob.NewEncoder(rw)
				err := encoder.Encode(*e)
				if err != nil {
					log.Println("Error encoding kafka.Messsage", err)
				}
				err = rw.Flush()
				if err != nil {
					log.Println("Flush failed", err)
				}
			} else if abort {
				return
			}
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "Error reading from Kafka %v \n", e)
			break
		}
	}
}
