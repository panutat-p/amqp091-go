package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	RABBITMQ_DSN      = "amqp://guest:guest@localhost:5672/"
	RABBITMQ_EXCHANGE = "fruit"
)

func main() {
	ctx := context.Background()
	conn, err := amqp.Dial(RABBITMQ_DSN)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		payload := map[string]any{
			"message": "🦊",
		}
		b, err := json.Marshal(payload)
		if err != nil {
			panic(err)
		}
		err = ch.PublishWithContext(
			ctx,
			RABBITMQ_EXCHANGE,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Headers: amqp.Table{
					"env": "local",
				},
				Body: b,
			},
		)
		if err != nil {
			fmt.Println("🔴 Failed to PublishWithContext, err:", err)
			panic(err)
		}
		fmt.Println(string(b))
	}()

	go func() {
		defer wg.Done()
		payload := map[string]any{
			"message": "🐵",
		}
		b, err := json.Marshal(payload)
		if err != nil {
			panic(err)
		}
		err = ch.PublishWithContext(
			ctx,
			RABBITMQ_EXCHANGE,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Headers: amqp.Table{
					"env": "local",
				},
				Body: b,
			},
		)
		if err != nil {
			fmt.Println("🔴 Failed to PublishWithContext, err:", err)
			panic(err)
		}
		fmt.Println(string(b))
	}()

	go func() {
		defer wg.Done()
		payload := map[string]any{
			"message": "🐢",
		}
		b, err := json.Marshal(payload)
		if err != nil {
			panic(err)
		}
		err = ch.PublishWithContext(
			ctx,
			RABBITMQ_EXCHANGE,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Headers: amqp.Table{
					"env": "local",
				},
				Body: b,
			},
		)
		if err != nil {
			fmt.Println("🔴 Failed to PublishWithContext, err:", err)
			panic(err)
		}
		fmt.Println(string(b))
	}()

	wg.Wait()
	fmt.Println("✅ done")
}
