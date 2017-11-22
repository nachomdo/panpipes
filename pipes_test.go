package main

import (
	"bufio"
	"encoding/gob"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestKafkaChannelToGobSink(t *testing.T) {
	events := make(chan kafka.Event)
	defer close(events)
	pr, pw := io.Pipe()
	predicates := []KafkaPredicate{&NoOpPredicate{}}
	rw := bufio.NewReadWriter(bufio.NewReader(pr), bufio.NewWriter(pw))
	go KafkaChannelToGobSink(events, rw, predicates)
	topic := "my-topic"

	msg := kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value: []byte("This is the payload"), Key: []byte("This is the key")}
	events <- &msg

	var data kafka.Message
	decoder := gob.NewDecoder(pr)
	err := decoder.Decode(&data)

	if err != nil {
		t.Errorf("Cannot decode original message %s", err)
	}

	if string(msg.Value) != string(data.Value) {
		t.Errorf("Read message does not match the original: %s", data.Value)
	}
}

func TestKafkaChannelToGobSinkWithPredicates(t *testing.T) {
	events := make(chan kafka.Event, 3)
	defer close(events)
	pr, pw := io.Pipe()
	defer pr.Close()
	predicates := []KafkaPredicate{&MessageCounter{MaxCount: 2}}
	rw := bufio.NewReadWriter(bufio.NewReader(pr), bufio.NewWriter(pw))
	go KafkaChannelToGobSink(events, rw, predicates)
	topic := "my-topic"

	msg := kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value: []byte("This is the payload"), Key: []byte("This is the key")}

	timeout := time.After(time.Millisecond * 500)
	var counter int32
	go func() {
		for {
			var data kafka.Message
			events <- &msg
			decoder := gob.NewDecoder(pr)
			err := decoder.Decode(&data)

			if err != nil {
				t.Skipf("Cannot decode original message %s", err)
			}
			atomic.AddInt32(&counter, 1)
		}
	}()

	<-timeout

	if atomic.LoadInt32(&counter) != 2 {
		t.Error("Unexpected message count: ", counter)
	}
}

func TestGobSourceKafkaChannel(t *testing.T) {
	pr, pw := io.Pipe()
	rw := bufio.NewReadWriter(bufio.NewReader(pr), bufio.NewWriter(pw))
	messages := make(chan *kafka.Message)
	topic := "my-topic"
	msg := kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value: []byte("This is the payload"), Key: []byte("This is the key")}

	encoder := gob.NewEncoder(rw)
	err := encoder.Encode(msg)

	if err != nil {
		t.Errorf("Cannot encode message %s", err)
	}

	go GobSourceToKafkaChannel("my-topic", rw.Reader, messages)
	rw.Flush()

	result := <-messages
	if string(result.Value) != string(msg.Value) {
		t.Errorf("Produced message does not match the original %s", result.Value)
	}
}

func TestRoundTrip(t *testing.T) {
	pr, pw := io.Pipe()
	events := make(chan kafka.Event)
	defer close(events)
	published := make(chan *kafka.Message)
	predicates := []KafkaPredicate{&NoOpPredicate{}}
	rw := bufio.NewReadWriter(bufio.NewReader(pr), bufio.NewWriter(pw))
	topic := "my-topic"
	msg := kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value: []byte("This is the payload"), Key: []byte("This is the key")}

	go KafkaChannelToGobSink(events, rw, predicates)
	go GobSourceToKafkaChannel(topic, rw.Reader, published)

	events <- &msg

	result := <-published
	if string(result.Value) != string(msg.Value) {
		t.Errorf("Produced message does not match the original %s", result.Value)
	}
}
