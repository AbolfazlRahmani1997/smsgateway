package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"masanger-worker/handler"
	initx "masanger-worker/init"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	if err := initx.LoadConfigFromJSON("config/config.json"); err != nil {
		log.Fatal("load config:", err)
	}

	brokers := initx.BrokersFromEnv()
	topic := os.Getenv("WORKER_TOPIC")
	group := os.Getenv("WORKER_GROUP")
	if topic == "" {
		log.Fatal("WORKER_TOPIC is empty")
	}
	if group == "" {
		log.Fatal("WORKER_GROUP is empty")
	}

	r := initx.NewReader(brokers, group, topic)
	ws := initx.NewWriter(brokers, os.Getenv("TOPIC_STATUS"))
	defer func() { _ = ws.Close() }()

	operator := envOr("OPERATOR", "mock")
	worker := envOr("WORKER_NAME", "w1")

	acc := handler.AtoiEnv("ACCEPT_LATENCY_MS", 50)
	min := handler.AtoiEnv("DELIVERY_MIN_MS", 300)
	max := handler.AtoiEnv("DELIVERY_MAX_MS", 1500)
	fail := handler.AtoiEnv("FAIL_RATIO_PCT", 10)

	w := handler.NewWorker(r, ws, operator, worker, acc, min, max, fail)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		w.Run(ctx)
	}()

	log.Printf("massger-worker started; topic=%s group=%s operator=%s worker=%s\n", topic, group, operator, worker)

	// 5) graceful shutdown
	waitForSignal()
	cancel()
	time.Sleep(300 * time.Millisecond)
}

func envOr(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}
func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
