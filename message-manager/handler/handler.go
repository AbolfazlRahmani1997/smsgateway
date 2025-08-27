package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	clientpb "message-manager/gen"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Message struct {
	ID         int       `gorm:"primaryKey" json:"id"`
	ClientID   string    `json:"client_id"`
	To         string    `json:"to"`
	Body       string    `json:"body"`
	Type       string    `json:"type"`
	PriceMinor int64     `json:"price_minor"`
	Status     string    `json:"status"`
	Operator   string    `json:"operator"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CreateMessageRequest struct {
	To   string `json:"to" binding:"required"`
	Body string `json:"body" binding:"required"`
	Type string `json:"type"` // NORMAL|PRIORITY
}
type CreateMessageResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}
type ErrorResponse struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

type DebitRequest struct {
	AmountMinor int64  `json:"amount_minor"`
	Ref         string `json:"ref"`
}
type DebitResponse struct {
	BalanceAfter int64 `json:"balance_after"`
}
type RefundRequest struct {
	AmountMinor int64  `json:"amount_minor"`
	Ref         string `json:"ref"`
}
type RefundResponse struct {
	BalanceAfter int64 `json:"balance_after"`
}

type StatusEvent struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`   // ACCEPTED|DELIVERED|FAILED|EXPIRED
	Operator  string `json:"operator"` // mock/mci/...
	At        string `json:"at"`
}

type API struct {
	DB            *gorm.DB
	WNormal       *kafka.Writer
	WPriority     *kafka.Writer
	RStatus       *kafka.Reader
	HTTP          *http.Client
	ClientMgrBase string
	PriceNormal   int64
	PricePriority int64
	CM            clientpb.ClientManagerClient
	ReadyQ        chan Message
}

func NewAPI(db *gorm.DB, wNormal, wPriority *kafka.Writer, rStatus *kafka.Reader, cm clientpb.ClientManagerClient, priceNormal, pricePriority int64) *API {
	return &API{
		DB: db, WNormal: wNormal, WPriority: wPriority, RStatus: rStatus,
		HTTP:        &http.Client{Timeout: 5 * time.Second},
		PriceNormal: priceNormal, PricePriority: pricePriority,
		CM:     cm,
		ReadyQ: make(chan Message),
	}
}

func (a *API) AutoMigrate() error {
	return a.DB.AutoMigrate(&Message{})
}

func (a *API) RegisterRoutes(r *gin.Engine) {
	r.GET("/healthz", func(c *gin.Context) { c.Status(200) })
	r.POST("/messages", a.CreateMessage)
	r.GET("/messages", a.ListMyMessages)
	r.GET("/messages/:id", a.GetMessage)
}

func atoi64(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }
func runeCount(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}
func newID() string { return time.Now().UTC().Format("20060102T150405.000000000") }

var errInsufficientFunds = errors.New("insufficient_funds")

func (a *API) PublishMessage() {
	for {
		select {
		case m := <-a.ReadyQ:
			val := map[string]any{"message_id": strconv.Itoa(m.ID), "client_id": m.ClientID, "to": m.To, "body": m.Body, "type": m.Type, "price": m.PriceMinor}
			b, _ := json.Marshal(val)
			kmsg := kafka.Message{Key: []byte(strconv.Itoa(m.ID)), Value: b, Headers: []kafka.Header{{Key: "x-msg-id", Value: []byte(strconv.Itoa(m.ID))}, {Key: "x-client-id", Value: []byte(m.ClientID)}, {Key: "x-type", Value: []byte(m.Type)}}}
			w := a.WNormal
			if m.Type == "PRIORITY" {
				w = a.WPriority
			}
			c := context.Background()
			if err := w.WriteMessages(c, kmsg); err != nil {
				_, _ = a.refund(c, m.ClientID, m.PriceMinor, strconv.Itoa(m.ID))
				return
			}
			if err := a.DB.Model(&Message{}).
				Where("id = ?", m.ID).
				Update("status", "QUEUED").Error; err != nil {
				log.Printf("gorm update failed: %v", err)
			}
		}
	}
}

func (a *API) debit(ctx context.Context, clientID string, amount int64, ref string) (int64, error) {
	if a.CM == nil {
		return 0, fmt.Errorf("client-manager grpc client not set")
	}
	resp, err := a.CM.Debit(ctx, &clientpb.MoneyRequest{
		ClientId:    clientID,
		AmountMinor: amount,
		Ref:         ref,
	})
	if err != nil {
		st, _ := status.FromError(err)
		if st != nil && st.Code() == codes.FailedPrecondition {
			return 0, errInsufficientFunds
		}
		return 0, err
	}
	return resp.GetBalanceAfter(), nil
}
func (a *API) refund(ctx context.Context, clientID string, amount int64, ref string) (int64, error) {
	if a.CM == nil {
		return 0, fmt.Errorf("client-manager grpc client not set")
	}
	resp, err := a.CM.Refund(ctx, &clientpb.MoneyRequest{
		ClientId:    clientID,
		AmountMinor: amount,
		Ref:         ref,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetBalanceAfter(), nil
}

func (a *API) CreateMessage(c *gin.Context) {
	clientID := c.GetHeader("X-Client-ID")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "missing X-Client-ID"})
		return
	}
	var req CreateMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	if req.Type == "" {
		req.Type = "NORMAL"
	}
	req.Type = strings.ToUpper(req.Type)
	if req.Type != "NORMAL" && req.Type != "PRIORITY" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "type must be NORMAL or PRIORITY"})
		return
	}
	if runeCount(req.Body) > 160 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "single-page only (<=160 chars)"})
		return
	}

	price := a.PriceNormal
	if req.Type == "PRIORITY" {
		price = a.PricePriority
	}

	if _, err := a.debit(c, clientID, price, ""); err != nil {
		if errors.Is(err, errInsufficientFunds) {
			c.JSON(http.StatusPaymentRequired, ErrorResponse{Error: "insufficient_funds"})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "internal_error", Detail: err.Error()})
		return
	}
	now := time.Now()
	m := &Message{ClientID: clientID, To: req.To, Body: req.Body, Type: req.Type, PriceMinor: price, Status: "CREATED", CreatedAt: now, UpdatedAt: now}
	if err := a.DB.Create(m).Error; err != nil {
		_, _ = a.refund(c, clientID, price, "")
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "internal_error", Detail: err.Error()})
		return
	}
	a.ReadyQ <- *m

	c.JSON(http.StatusCreated, CreateMessageResponse{ID: strconv.Itoa(m.ID), Status: "CREATED"})
}

func (a *API) ListMyMessages(c *gin.Context) {
	clientID := c.GetHeader("X-Client-ID")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing_client_id", "detail": "X-Client-ID header required"})
		return
	}
	limit := 20
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 100 {
				n = 100
			}
			limit = n
		}
	}
	page := 0
	if v := c.Query("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			page = n
		}
	}
	fType := strings.ToUpper(strings.TrimSpace(c.Query("type")))     // NORMAL | PRIORITY
	fStatus := strings.ToUpper(strings.TrimSpace(c.Query("status"))) // QUEUED|ACCEPTED|...

	q := a.DB.Where("client_id = ?", clientID)

	if fType == "NORMAL" || fType == "PRIORITY" {
		q = q.Where("type = ?", fType)
	}
	switch fStatus {
	case "QUEUED", "ACCEPTED", "DELIVERED", "FAILED", "EXPIRED":
		q = q.Where("status = ?", fStatus)
	}

	var msgs []Message
	if err := q.
		Order("created_at DESC").
		Limit(limit).
		Offset(page * limit).
		Find(&msgs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db_error", "detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items": msgs,
		"page":  page,
		"limit": limit,
		"count": len(msgs),
	})
}

func (a *API) GetMessage(c *gin.Context) {
	id := c.Param("id")
	var m Message
	if err := a.DB.First(&m, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "not_found"})
		return
	}
	c.JSON(http.StatusOK, m)
}

func (a *API) StartStatusConsumer(ctx context.Context) {
	if a.RStatus == nil {
		return
	}
	go func() {
		defer a.RStatus.Close()
		for {
			m, err := a.RStatus.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Println("status read err:", err)
				continue
			}
			var evt StatusEvent
			if err := json.Unmarshal(m.Value, &evt); err != nil {
				log.Println("status json err:", err)
				continue
			}
			s := strings.ToUpper(evt.Status)

			if err := a.DB.Transaction(func(tx *gorm.DB) error {
				var msg Message
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&msg, "id = ?", evt.MessageID).Error; err != nil {
					return err
				}
				// final check
				switch strings.ToUpper(msg.Status) {
				case "DELIVERED", "FAILED", "EXPIRED":
					return nil
				}
				msg.Status = s
				if evt.Operator != "" {
					msg.Operator = evt.Operator
				}
				msg.UpdatedAt = time.Now()
				if err := tx.Save(&msg).Error; err != nil {
					return err
				}
				if s == "FAILED" || s == "EXPIRED" {
					_, err := a.refund(ctx, msg.ClientID, msg.PriceMinor, "")
					if err != nil {
						log.Println("refund error:", err)
					}
				}
				return nil
			}); err != nil {
				log.Println("status apply tx err:", err)
			}
		}
	}()
}
