package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type KafkaPredicate interface {
	isValid(*kafka.Message) bool
	abortOnFailure() bool
}

type MessageCounter struct {
	CurrentCount int
	MaxCount     int
}

type NoOpPredicate struct{}

func (*NoOpPredicate) isValid(msg *kafka.Message) bool {
	return true
}

func (*NoOpPredicate) abortOnFailure() bool { return false }

func (c *MessageCounter) isValid(msg *kafka.Message) bool {
	c.CurrentCount++
	if c.CurrentCount <= c.MaxCount {
		return true
	} else {
		return false
	}
}

func (*MessageCounter) abortOnFailure() bool { return true }

func AllValid(msg *kafka.Message, predicates []KafkaPredicate) (bool, bool) {
	guard, abort := true, false

	for _, p := range predicates {
		guard = guard && p.isValid(msg)
		abort = abort || p.abortOnFailure()
	}

	return guard, abort
}

type TimestampRestriction struct {
	MinDate time.Time
	MaxDate time.Time
}

func (tr *TimestampRestriction) isValid(msg *kafka.Message) bool {
	if msg.TimestampType != kafka.TimestampNotAvailable {
		return msg.Timestamp.After(tr.MinDate) && msg.Timestamp.Before(tr.MaxDate)
	} else {
		return false
	}
}

func (*TimestampRestriction) abortOnFailure() bool { return false }
