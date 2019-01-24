package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pangpanglabs/goutils/kafka"
)

//If you need to call the producer multiple times in a loop,
// in order to save server resources, it is recommended to make a single case.
var kafkaSampleInstance *KafkaSample
var kafkaSampleOnce sync.Once

type KafkaSample struct {
	Producer *kafka.Producer
}

type Config struct {
	Brokers []string
	Topic   string
}

func (KafkaSample) GetInstance(config *Config) *KafkaSample {
	kafkaSampleOnce.Do(func() {
		if p, errd := kafka.NewProducer(config.Brokers, config.Topic, func(c *sarama.Config) {
			c.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
			c.Producer.Compression = sarama.CompressionGZIP     // Compress messages
			c.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

		}); errd != nil {
			// to be
			fmt.Println(errd)
			return
		} else {
			kafkaSampleInstance = &KafkaSample{
				Producer: p,
			}
		}
	})
	return kafkaSampleInstance
}
