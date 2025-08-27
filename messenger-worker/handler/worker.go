package handler

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type InMsg struct {
	MessageID string `json:"message_id"`
	ClientID  string `json:"client_id"`
	To        string `json:"to"`
	Body      string `json:"body"`
	Type      string `json:"type"`
	Price     int64  `json:"price"`
	CreatedAt string `json:"created_at"`
}

type StatusEvt struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`   // ACCEPTED | DELIVERED | FAILED | EXPIRED
	Operator  string `json:"operator"` // mci|mtn|mock|...
	At        string `json:"at"`
	TraceID   string `json:"trace_id"`
	Worker    string `json:"worker"`
}

// ===== Worker (single topic) =====

type Worker struct {
	R        *kafka.Reader
	WStatus  *kafka.Writer
	Operator string
	Worker   string

	AcceptLatency time.Duration
	DeliverMin    time.Duration
	DeliverMax    time.Duration
	FailRatio     int
}

func NewWorker(r *kafka.Reader, ws *kafka.Writer,
	operator, worker string, acceptMs, minMs, maxMs, failPct int,
) *Worker {
	return &Worker{
		R:             r,
		WStatus:       ws,
		Operator:      operator,
		Worker:        worker,
		AcceptLatency: time.Duration(acceptMs) * time.Millisecond,
		DeliverMin:    time.Duration(minMs) * time.Millisecond,
		DeliverMax:    time.Duration(maxMs) * time.Millisecond,
		FailRatio:     failPct,
	}
}

func (w *Worker) Run(ctx context.Context) {
	w.loop(ctx, w.R)
}

func (w *Worker) loop(ctx context.Context, r *kafka.Reader) {
	defer r.Close()
	log.Printf("[worker] consuming topic=%s group=%s\n", r.Config().Topic, r.Config().GroupID)

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[worker] read err: %v\n", err)
			continue
		}
		var in InMsg
		if err := json.Unmarshal(msg.Value, &in); err != nil {
			log.Printf("[worker] bad json: %v\n", err)
			continue
		}
		trace := uuid.NewString()

		// Accept fast
		time.Sleep(w.AcceptLatency)
		_ = w.publish(ctx, StatusEvt{
			MessageID: in.MessageID,
			Status:    "ACCEPTED",
			Operator:  w.Operator,
			At:        time.Now().UTC().Format(time.RFC3339Nano),
			TraceID:   trace,
			Worker:    w.Worker,
		})

		// Final status
		delay := w.randomDelay()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		status := "DELIVERED"
		if w.shouldFail() {
			status = "FAILED"
		}

		if err := w.publish(ctx, StatusEvt{
			MessageID: in.MessageID,
			Status:    status,
			Operator:  w.Operator,
			At:        time.Now().UTC().Format(time.RFC3339Nano),
			TraceID:   trace,
			Worker:    w.Worker,
		}); err != nil {
			log.Printf("[worker] publish err: %v\n", err)
		}
	}
}

func (w *Worker) publish(ctx context.Context, evt StatusEvt) error {
	b, _ := json.Marshal(evt)
	return w.WStatus.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.MessageID),
		Value: b,
		Headers: []kafka.Header{
			{Key: "x-msg-id", Value: []byte(evt.MessageID)},
			{Key: "x-status", Value: []byte(evt.Status)},
			{Key: "x-operator", Value: []byte(evt.Operator)},
			{Key: "x-trace-id", Value: []byte(evt.TraceID)},
			{Key: "x-worker", Value: []byte(evt.Worker)},
		},
	})
}

func (w *Worker) randomDelay() time.Duration {
	if w.DeliverMax <= w.DeliverMin {
		return w.DeliverMin
	}
	delta := w.DeliverMax - w.DeliverMin
	return w.DeliverMin + time.Duration(rand.Int63n(int64(delta)))
}

func (w *Worker) shouldFail() bool {
	if w.FailRatio <= 0 {
		return false
	}
	if w.FailRatio >= 100 {
		return true
	}
	return rand.Intn(100) < w.FailRatio
}

// ===== Helpers =====
func AtoiEnv(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
