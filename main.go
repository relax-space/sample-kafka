package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/labstack/echo"

	"syscall"
)

func main() {
	e := echo.New()

	config := Config{
		Brokers: []string{
			"127.0.0.1:9092",
		},
		Topic: "sample-kafka",
	}

	e.GET("/ping", func(c echo.Context) error {
		return c.String(http.StatusOK, "pong")
	})
	e.POST("/kafka", func(c echo.Context) error {
		var dto = TestDto{
			"xiao",
			18,
		}
		KafkaSample{}.GetInstance(&config).Producer.Send(&dto)
		fmt.Println("kafka producer")

		return c.String(http.StatusOK, "success")
	})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, os.Interrupt)
	go func() {
		for s := range signals {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				os.Exit(0)
			}
		}
	}()

	go func(k Config) {
		Consumer(&k)
	}(config)

	if err := e.Start(":8080"); err != nil {
		log.Println(err)
	}
}
