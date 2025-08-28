package main

import (
	grpcserver "client-manager/grpc"
	initx "client-manager/init"
	"google.golang.org/grpc"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"net"
	"os"

	"client-manager/config"
	"client-manager/handler"
)

func main() {
	if err := config.LoadConfigFromJSON("config/config.json"); err != nil {
		log.Fatal("cannot load config.json:", err)
	}
	db, err := gorm.Open(mysql.Open(os.Getenv("DATABASE_URL")), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	err = initx.Run(db)
	if err != nil {

	}
	svc := handler.New(db)
	_ = svc.(*handler.Svc).AutoMigrate()

	lis, _ := net.Listen("tcp", "0.0.0.0:"+os.Getenv("GRPC_PORT"))
	gs := grpc.NewServer()
	grpcserver.Register(gs, svc)
	log.Println("gRPC on :" + os.Getenv("GRPC_PORT"))
	log.Fatal(gs.Serve(lis))
}
