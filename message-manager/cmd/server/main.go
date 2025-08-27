package main

import (
	"context"
	"log"
	clientpb "message-manager/gen"
	"message-manager/handler"
	initx "message-manager/init"
	"os"

	"github.com/gin-gonic/gin"
)

func main() {

	if err := initx.LoadConfigFromJSON("config/config.json"); err != nil {
		log.Fatal("load config:", err)
	}

	db, err := initx.OpenDBFromEnv()
	if err != nil {
		log.Fatal("db connect:", err)
	}

	brokers := initx.BrokersFromEnv()
	wNormal := initx.NewWriter(brokers, os.Getenv("TOPIC_NORMAL"))
	wPriority := initx.NewWriter(brokers, os.Getenv("TOPIC_PRIORITY"))
	rStatus := initx.NewReader(brokers, os.Getenv("GROUP_STATUS"), os.Getenv("TOPIC_STATUS"))

	cmAddr := os.Getenv("CLIENT_MANAGER_GRPC_ADDR")
	grpcConn, err := initx.NewGRPCClient(cmAddr)
	if err != nil {
		log.Fatal("grpc dial:", err)
	}
	defer grpcConn.Close()

	cmClient := clientpb.NewClientManagerClient(grpcConn)

	priceNormal := atoi64(os.Getenv("PRICE_NORMAL"))
	pricePriority := atoi64(os.Getenv("PRICE_PRIORITY"))

	api := handler.NewAPI(db, wNormal, wPriority, rStatus, cmClient, priceNormal, pricePriority)
	go api.PublishMessage()
	if err := api.AutoMigrate(); err != nil {
		log.Fatal("migrate:", err)
	}
	api.StartStatusConsumer(context.Background())
	r := gin.Default()
	api.RegisterRoutes(r)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Println("massage-manager listening on :" + port)
	if err := r.Run("0.0.0.0:" + port); err != nil {
		log.Fatal(err)
	}
}

// tiny helper (avoid importing strconv here)
func atoi64(s string) int64 {
	var n int64
	for _, ch := range []byte(s) {
		if ch < '0' || ch > '9' {
			return n
		}
		n = n*10 + int64(ch-'0')
	}
	return n
}
