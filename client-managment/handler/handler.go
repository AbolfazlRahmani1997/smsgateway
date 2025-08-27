package handler

import (
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrNotFound          = gorm.ErrRecordNotFound
	ErrInsufficientFunds = errors.New("insufficient_funds")
)

type Client struct {
	ClientID     string `gorm:"primaryKey"`
	BalanceMinor int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
type PricePlan struct {
	ClientID           string `gorm:"primaryKey"`
	NormalPriceMinor   int64
	PriorityPriceMinor int64
}
type Transaction struct {
	ID          uint `gorm:"primaryKey"`
	ClientID    string
	AmountMinor int64  // +refund, -debit
	Type        string // DEBIT|REFUND
	Ref         string
	CreatedAt   time.Time
}

type Service interface {
	CreateClient(clientID string, initial, normalPrice, priorityPrice int64) error
	GetClient(clientID string) (Client, error)
	GetPricePlan(clientID string) (PricePlan, error)
	Debit(clientID string, amount int64, ref string) (balanceAfter int64, err error)
	Refund(clientID string, amount int64, ref string) (balanceAfter int64, err error)
}

type Svc struct{ db *gorm.DB }

func New(db *gorm.DB) Service { return &Svc{db: db} }

func (s *Svc) AutoMigrate() error {
	return s.db.AutoMigrate(&Client{}, &PricePlan{}, &Transaction{})
}

func (s *Svc) CreateClient(id string, initial, normal, priority int64) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "client_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"balance_minor", "updated_at"}),
		}).Create(&Client{ClientID: id, BalanceMinor: initial, UpdatedAt: time.Now()}).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "client_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"normal_price_minor", "priority_price_minor"}),
		}).Create(&PricePlan{ClientID: id, NormalPriceMinor: normal, PriorityPriceMinor: priority}).Error
	})
}

func (s *Svc) GetClient(id string) (Client, error) {
	var c Client
	return c, s.db.First(&c, "client_id = ?", id).Error
}

func (s *Svc) GetPricePlan(id string) (PricePlan, error) {
	var p PricePlan
	return p, s.db.First(&p, "client_id = ?", id).Error
}

func (s *Svc) Debit(id string, amount int64, ref string) (int64, error) {
	var after int64
	err := s.db.Transaction(func(tx *gorm.DB) error {
		var c Client
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&c, "client_id = ?", id).Error; err != nil {
			return err
		}
		if c.BalanceMinor < amount {
			return ErrInsufficientFunds
		}
		c.BalanceMinor -= amount
		if err := tx.Save(&c).Error; err != nil {
			return err
		}
		after = c.BalanceMinor
		return tx.Create(&Transaction{ClientID: id, AmountMinor: -amount, Type: "DEBIT", Ref: ref}).Error
	})
	return after, err
}

func (s *Svc) Refund(id string, amount int64, ref string) (int64, error) {
	var after int64
	err := s.db.Transaction(func(tx *gorm.DB) error {
		var c Client
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&c, "client_id = ?", id).Error; err != nil {
			return err
		}
		c.BalanceMinor += amount
		if err := tx.Save(&c).Error; err != nil {
			return err
		}
		after = c.BalanceMinor
		return tx.Create(&Transaction{ClientID: id, AmountMinor: amount, Type: "REFUND", Ref: ref}).Error
	})
	return after, err
}
