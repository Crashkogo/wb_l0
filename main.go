package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	stan "github.com/nats-io/stan.go"
	log "log"
	"os"
	"sync"
	"time"
	//"github.com/shijuvar/gokit/examples/nats-streaming/pb"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var sqlmsg ordWB
var err error
var retID int8
var wg sync.WaitGroup

const (
	clusterID = "wb_cluster"
	clientID  = "wbID"
	channel   = "wb_channel"
)

func main() {
	sc, err := stan.Connect(clusterID, clientID)

	if err != nil {
		log.Fatal(err)
	}
	// Simple Synchronous Publisher
	err = sc.Publish(channel, []byte("{\n  \"order_uid\": \"b563feb7b2b84b6test\",\n  \"track_number\": \"WBILMTESTTRACK\",\n  \"entry\": \"WBIL\",\n  \"delivery\": {\n    \"name\": \"Test Testov\",\n    \"phone\": \"+9720000000\",\n    \"zip\": \"2639809\",\n    \"city\": \"Kiryat Mozkin\",\n    \"address\": \"Ploshad Mira 15\",\n    \"region\": \"Kraiot\",\n    \"email\": \"test@gmail.com\"\n  },\n  \"payment\": {\n    \"transaction\": \"b563feb7b2b84b6test\",\n    \"request_id\": \"\",\n    \"currency\": \"USD\",\n    \"provider\": \"wbpay\",\n    \"amount\": 1817,\n    \"payment_dt\": 1637907727,\n    \"bank\": \"alpha\",\n    \"delivery_cost\": 1500,\n    \"goods_total\": 317,\n    \"custom_fee\": 0\n  },\n  \"items\": [\n    {\n      \"chrt_id\": 9934930,\n      \"track_number\": \"WBILMTESTTRACK\",\n      \"price\": 453,\n      \"rid\": \"ab4219087a764ae0btest\",\n      \"name\": \"Mascaras\",\n      \"sale\": 30,\n      \"size\": \"0\",\n      \"total_price\": 317,\n      \"nm_id\": 2389212,\n      \"brand\": \"Vivienne Sabo\",\n      \"status\": 202\n    }\n  ],\n  \"locale\": \"en\",\n  \"internal_signature\": \"\",\n  \"customer_id\": \"test\",\n  \"delivery_service\": \"meest\",\n  \"shardkey\": \"9\",\n  \"sm_id\": 99,\n  \"date_created\": \"2021-11-26T06:22:19Z\",\n  \"oof_shard\": \"1\"\n}")) // does not return until an ack has been received from NATS Streaming
	if err != nil {
		log.Println(err)
	}
	// Close connection
	defer sc.Close()
	wg.Add(1)
	go func() {
		sub, err := sc.Subscribe(channel, func(m *stan.Msg) {

			err = json.Unmarshal(m.Data, &sqlmsg)
			if err != nil {
				log.Println(err)
			}
			fmt.Println(sqlmsg.Delivery.Name)
			db, err := sql.Open("pgx", "postgres://postgres:Parol123!@localhost:5432/wb_l0")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
				os.Exit(1)
			}
			err = db.QueryRow("INSERT INTO orders(id,order_uid,track_number,entry,locale,internal_signature,customer_id,delivery_service,shardkey,sm_id,date_created,oof_shard) VALUES (default, $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id", sqlmsg.OrderUID, sqlmsg.TrackNumber, sqlmsg.Entry, sqlmsg.Locale, sqlmsg.InternalSignature, sqlmsg.CustomerID, sqlmsg.DeliveryService, sqlmsg.Shardkey, sqlmsg.SmID, sqlmsg.DateCreated, sqlmsg.OofShard).Scan(&retID)
			if err != nil {
				log.Println(err)
			}
			_, err = db.Exec("INSERT INTO delivery(id,name,phone,zip,city,adress,region,email,orderid) VALUES (default,$1, $2, $3, $4, $5, $6, $7, $8)", sqlmsg.Delivery.Name, sqlmsg.Delivery.Phone, sqlmsg.Delivery.Zip, sqlmsg.Delivery.City, sqlmsg.Delivery.Address, sqlmsg.Delivery.Region, sqlmsg.Delivery.Email, retID)
			if err != nil {
				log.Println(err)
			}
			_, err = db.Exec("INSERT INTO payment(id,transaction,request_id,currency,provider,amount,payment_dt,bank,delivery_cost,goods_total,custom_fee,orderid) VALUES (default,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", sqlmsg.Payment.Transaction, sqlmsg.Payment.RequestID, sqlmsg.Payment.Currency, sqlmsg.Payment.Provider, sqlmsg.Payment.Amount, sqlmsg.Payment.PaymentDt, sqlmsg.Payment.Bank, sqlmsg.Payment.DeliveryCost, sqlmsg.Payment.GoodsTotal, sqlmsg.Payment.CustomFee, retID)
			if err != nil {
				log.Println(err)
			}
			_, err = db.Exec("INSERT INTO items(id,chrt_id,track_number,price,rid,name,sale,size,total_price,nm_id,brand,status,orderid) VALUES (default,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", sqlmsg.Items[0].ChrtID, sqlmsg.Items[0].TrackNumber, sqlmsg.Items[0].Price, sqlmsg.Items[0].Rid, sqlmsg.Items[0].Name, sqlmsg.Items[0].Sale, sqlmsg.Items[0].Size, sqlmsg.Items[0].TotalPrice, sqlmsg.Items[0].NmID, sqlmsg.Items[0].Brand, sqlmsg.Items[0].Status, retID)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()

			defer db.Close()
		}, stan.StartWithLastReceived())
		if err != nil {
			log.Println(err)
		}
		//res, err := db.Exec //работа с SQL -
		defer sub.Unsubscribe()

	}()

	wg.Wait()
	fmt.Println("Обработка выполнена!")
}

type ordWB struct {
	OrderUID    string `json:"order_uid"`
	TrackNumber string `json:"track_number"`
	Entry       string `json:"entry"`
	Delivery    struct {
		Name    string `json:"name"`
		Phone   string `json:"phone"`
		Zip     string `json:"zip"`
		City    string `json:"city"`
		Address string `json:"address"`
		Region  string `json:"region"`
		Email   string `json:"email"`
	} `json:"delivery"`
	Payment struct {
		Transaction  string `json:"transaction"`
		RequestID    string `json:"request_id"`
		Currency     string `json:"currency"`
		Provider     string `json:"provider"`
		Amount       int    `json:"amount"`
		PaymentDt    int    `json:"payment_dt"`
		Bank         string `json:"bank"`
		DeliveryCost int    `json:"delivery_cost"`
		GoodsTotal   int    `json:"goods_total"`
		CustomFee    int    `json:"custom_fee"`
	} `json:"payment"`
	Items []struct {
		ChrtID      int    `json:"chrt_id"`
		TrackNumber string `json:"track_number"`
		Price       int    `json:"price"`
		Rid         string `json:"rid"`
		Name        string `json:"name"`
		Sale        int    `json:"sale"`
		Size        string `json:"size"`
		TotalPrice  int    `json:"total_price"`
		NmID        int    `json:"nm_id"`
		Brand       string `json:"brand"`
		Status      int    `json:"status"`
	} `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerID        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmID              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}
