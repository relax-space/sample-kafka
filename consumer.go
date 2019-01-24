package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/pangpanglabs/goutils/kafka"
)

type TestDto struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func Consumer(k *Config) {
	consumer, err := kafka.NewConsumer(k.Brokers, k.Topic, nil, sarama.OffsetNewest, func(c *sarama.Config) {

	})
	if err != nil {
		panic(err)
	}
	messages, err := consumer.Messages()

	if err != nil {
		panic(err)
	}
	for m := range messages {
		var v TestDto
		d := json.NewDecoder(bytes.NewReader(m.Value))
		d.UseNumber()
		err := d.Decode(&v)
		if err != nil {
			//log.Println(err)
			fmt.Println(err)
		}
		fmt.Println("kafka consumered", v)
		//fmt.Printf("consumer=>%+v", v)
	}
}
