package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	stan "github.com/nats-io/stan.go"
	"html/template"
	log "log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var sqlmsg ordWB
var tpl = template.Must(template.ParseFiles("index.html"))
var retID int8
var jsonMsg []byte

const (
	clusterID = "wb_cluster"
	clientID  = "wbID"
	channel   = "wb_channel"
)

func main() {
	//инициализируем кэш
	myCache := New(0*time.Minute, 0*time.Minute)
	//Подключаемся к базе данных
	db, err := sql.Open("pgx", "postgres://postgres:Parol123!@localhost:5432/wbl0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	//Заполняем кэш +
	rows, err := db.Query("SELECT * FROM orders")
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ResultID int8
		var ResultUID string
		var ResultMSG string
		err = rows.Scan(&ResultID, &ResultUID, &ResultMSG)
		jsonMsg = []byte(ResultMSG)
		err = json.Unmarshal(jsonMsg, &sqlmsg)
		myCache.Set(ResultID, sqlmsg, 0*time.Minute)
	}
	//Заполняем кэш -

	//Подключаемся к Nats
	sc, err := stan.Connect(clusterID, clientID)
	if err != nil {
		log.Println(err)
	}
	//Подписываемся на получение сообщений в NATS
	sub, err := sc.Subscribe(channel, func(m *stan.Msg) {

		err = json.Unmarshal(m.Data, &sqlmsg)
		if err != nil {
			log.Println(err)
			fmt.Println("Неверный формат сообщения")
		} else {
			err = db.QueryRow("INSERT INTO orders(id,uid,message) VALUES (default, $1,$2) RETURNING id", sqlmsg.OrderUID, m.Data).Scan(&retID)
			if err != nil {
				log.Println(err)
			}
			//делаем запись в кэш
			myCache.Set(retID, sqlmsg, 0*time.Minute)
		}
	}, stan.StartWithLastReceived())
	if err != nil {
		log.Println(err)
	}

	defer sub.Unsubscribe()
	defer sc.Close()

	//Запуск и работа веб сервера
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tpl.Execute(w, nil)
	})
	//Реализация помска по кэшу
	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {

		sValue := r.FormValue("q")
		tempRetid, _ := strconv.ParseInt(sValue, 10, 0)
		retID = int8(tempRetid)
		fmt.Println(retID)
		message, haveСache := myCache.Get(retID)
		if haveСache == true {
			cMsg := message.(ordWB)
			fmt.Fprintf(w, "Номер сообщения: %v \n", retID)
			fmt.Fprintf(w, "MAIN: order_uid: %v, track_number: %v, entry: %v, locale: %v, internal_signature:%v, customer_id:%v, delivery_service:%v, shardkey: %v, sm_id: %v, date_created:%v, oof_shard: %v\n", cMsg.OrderUID, cMsg.TrackNumber, cMsg.Entry, cMsg.Locale, cMsg.InternalSignature, cMsg.CustomerID, cMsg.DeliveryService, cMsg.Shardkey, cMsg.SmID, cMsg.DateCreated, cMsg.OofShard)
			fmt.Fprintf(w, "DELIVERY: name: %v, phone: %v, zip: %v, city: %v, adress: %v, region: %v, email: %v \n", cMsg.Delivery.Name, cMsg.Delivery.Phone, cMsg.Delivery.Zip, cMsg.Delivery.City, cMsg.Delivery.Address, cMsg.Delivery.Region, cMsg.Delivery.Email)
			fmt.Fprintf(w, "PAYMENT: transaction: %v, request_id: %v, currency: %v, provider: %v, amount: %v, payment_dt: %v, bank: %v, delivery_cost: %v, goods_total: %v, custom_fee: %v\n", cMsg.Payment.Transaction, cMsg.Payment.RequestID, cMsg.Payment.Currency, cMsg.Payment.Provider, cMsg.Payment.Amount, cMsg.Payment.PaymentDt, cMsg.Payment.Bank, cMsg.Payment.DeliveryCost, cMsg.Payment.GoodsTotal, cMsg.Payment.CustomFee)
			for len := range cMsg.Items {
				fmt.Fprintf(w, "ITEMS: chrt_id: %v, track_number: %v, price: %v, rid: %v, name: %v, sale: %v, size: %v, total_price: %v, nm_id: %v, brand: %v, status: %v\n", cMsg.Items[len].ChrtID, cMsg.Items[len].TrackNumber, cMsg.Items[len].Price, cMsg.Items[len].Rid, cMsg.Items[len].Name, cMsg.Items[len].Sale, cMsg.Items[len].Size, cMsg.Items[len].TotalPrice, cMsg.Items[len].NmID, cMsg.Items[len].Brand, cMsg.Items[len].Status)
			}
		} else {
			fmt.Fprintf(w, "Нет такой записи")
		}
	})
	http.ListenAndServe(":80", nil)
	//Конец веб сервер
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

func New(defaultExpiration, cleanupInterval time.Duration) *Cache {

	// инициализируем карту(map) в паре ключ(string)/значение(Item)
	items := make(map[int8]Item)

	cache := Cache{
		items:             items,
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
	}

	// Если интервал очистки больше 0, запускаем GC (удаление устаревших элементов)
	if cleanupInterval > 0 {
		cache.StartGC() // данный метод рассматривается ниже
	}

	return &cache
}
func (c *Cache) Set(key int8, value interface{}, duration time.Duration) {

	var expiration int64

	// Если продолжительность жизни равна 0 - используется значение по-умолчанию
	if duration == 0 {
		duration = c.defaultExpiration
	}

	// Устанавливаем время истечения кеша
	if duration > 0 {
		expiration = time.Now().Add(duration).UnixNano()
	}

	c.Lock()

	defer c.Unlock()

	c.items[key] = Item{
		Value:      value,
		Expiration: expiration,
		Created:    time.Now(),
	}

}
func (c *Cache) Get(key int8) (interface{}, bool) {

	c.RLock()

	defer c.RUnlock()

	item, found := c.items[key]

	// ключ не найден
	if !found {
		return nil, false
	}

	// Проверка на установку времени истечения, в противном случае он бессрочный
	if item.Expiration > 0 {

		// Если в момент запроса кеш устарел возвращаем nil
		if time.Now().UnixNano() > item.Expiration {
			return nil, false
		}

	}

	return item.Value, true
}
func (c *Cache) Delete(key int8) error {

	c.Lock()

	defer c.Unlock()

	if _, found := c.items[key]; !found {
		return errors.New("Key not found")
	}

	delete(c.items, key)

	return nil
}

type Cache struct {
	sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	items             map[int8]Item
}
type Item struct {
	Value      interface{}
	Created    time.Time
	Expiration int64
}

func (c *Cache) StartGC() {
	go c.GC()
}

func (c *Cache) GC() {

	for {
		// ожидаем время установленное в cleanupInterval
		<-time.After(c.cleanupInterval)

		if c.items == nil {
			return
		}

		// Ищем элементы с истекшим временем жизни и удаляем из хранилища
		if keys := c.expiredKeys(); len(keys) != 0 {
			c.clearItems(keys)

		}

	}

}

// expiredKeys возвращает список "просроченных" ключей
func (c *Cache) expiredKeys() (keys []int8) {

	c.RLock()

	defer c.RUnlock()

	for k, i := range c.items {
		if time.Now().UnixNano() > i.Expiration && i.Expiration > 0 {
			keys = append(keys, k)
		}
	}

	return
}

// clearItems удаляет ключи из переданного списка, в нашем случае "просроченные"
func (c *Cache) clearItems(keys []int8) {

	c.Lock()

	defer c.Unlock()

	for _, k := range keys {
		delete(c.items, k)
	}
}
