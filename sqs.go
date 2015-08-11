package main

import (
	"log"
	"os"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/sqs"
)

// SQS is a bit different compared to rabbitmq and ironmq in terms of limitations.
// First off, the maximum batch size for messages is 10, and the maximum single message size is 64kb.
// If you want to send the maximum message size of 256kb, you have to fragment the message apart into 4
// seperate 64kb chunks.

type SqsRunner struct {
	conn *sqs.SQS
}

func (s *SqsRunner) setupQueues(queues []string) {
	auth := aws.Auth{AccessKey: os.Getenv("AWS_ACCESS_KEY"), SecretKey: os.Getenv("AWS_SECRET_KEY")}
	s.conn = sqs.New(auth, aws.USEast)
	for _, m := range queues {
		_, err := s.conn.CreateQueue(m)
		if err != nil {
			log.Println(err)
		}
	}
}

func (s *SqsRunner) Name() string {
	return "SQS"
}

func (s *SqsRunner) Produce(name, body string, messages int) {
	q, err := s.conn.GetQueue(name)
	if err != nil {
		log.Println(err)
		return
	}
	msgs := make([]string, messages)
	for i := 0; i < messages; i++ {
		msgs[i] = body
	}
	_, err = q.SendMessageBatchString(msgs)
	if err != nil {
		log.Println(err)
	}
}

func (s *SqsRunner) Consume(name string, messages int) {
	q, err := s.conn.GetQueue(name)
	if err != nil {
		log.Println(err)
		return
	}
	res, err := q.ReceiveMessage(messages)
	if err != nil {
		log.Println(err)
		return
	}
	q.DeleteMessageBatch(res.Messages)
}
