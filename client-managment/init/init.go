package initx

import (
	"os"
	"strconv"
	"time"

	"gorm.io/gorm"
)

// Run performs automigrate and optional seeding.
func Run(db *gorm.DB) error {
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
		AmountMinor int64
		Type        string
		Ref         string
		CreatedAt   time.Time
	}

	if err := db.AutoMigrate(&Client{}, &PricePlan{}, &Transaction{}); err != nil {
		return err
	}

	if os.Getenv("SEED_DEMO") == "1" {
		cid := os.Getenv("DEMO_CLIENT_ID")
		bal := atoi64(os.Getenv("DEMO_BALANCE"))
		nPrice := atoi64(os.Getenv("DEMO_NORMAL_PRICE"))
		pPrice := atoi64(os.Getenv("DEMO_PRIORITY_PRICE"))

		_ = db.Save(&Client{ClientID: cid, BalanceMinor: bal, UpdatedAt: time.Now()}).Error
		_ = db.Save(&PricePlan{ClientID: cid, NormalPriceMinor: nPrice, PriorityPriceMinor: pPrice}).Error
	}
	return nil
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
