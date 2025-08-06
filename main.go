package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		slog.Error("Error running Kafka demo", "error", err)
		os.Exit(1)
	}
	slog.Info("Kafka demo completed successfully")
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || (len(brokers) == 1 && brokers[0] == "") {
		return fmt.Errorf("KAFKA_BROKERS environment variable is not set or empty")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("kafka-demo"),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return producer(ctx, client)
	})
	g.Go(func() error {
		return consumer(ctx, client)
	})
	if err := g.Wait(); err != nil {
		slog.Error("Error in producer or consumer", "error", err)
		return err
	}
	return nil
}

func producer(ctx context.Context, cl *kgo.Client) error {
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			// Simulate producing messages
			msg := &kgo.Record{
				Topic: "test-topic",
				Value: []byte("Hello, Kafka!"),
				Key:   []byte("key"),
			}
			res := cl.ProduceSync(ctx, msg)
			if err := res.FirstErr(); err != nil {
				return fmt.Errorf("failed to produce message: %w", err)
			}

			slog.InfoContext(ctx, "Produced message",
				"topic", msg.Topic,
				"key", string(msg.Key),
				"value", string(msg.Value),
			)
		}
	}
}

func consumer(ctx context.Context, cl *kgo.Client) error {
	cl.AddConsumeTopics("test-topic")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Simulate consuming messages
			fetches := cl.PollFetches(ctx)
			if fetches.Err() != nil {
				return fetches.Err()
			}

			fetches.EachRecord(func(r *kgo.Record) {
				slog.InfoContext(ctx, "Consumed message",
					"topic", r.Topic,
					"key", string(r.Key),
					"value", string(r.Value),
				)
			})
		}
	}
}
