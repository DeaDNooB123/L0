package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"

	memcache "github.com/maxchagin/go-memorycache-example"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "petuhov_n"
	password = "qwerty"
	dbname   = "L0_DB"
)

type order struct {
	id   string
	data []byte
}

var Numid string

func main() {
	nc, e := nats.Connect(nats.DefaultURL)
	if e != nil {
		log.Fatal(e)
	}
	defer nc.Drain()

	js, e := nc.JetStream()
	if e != nil {
		log.Fatal(e)
	}

	fmt.Println("jetstream context:", js)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ordersFromStream := getDataFromStream(js)

	for _, v := range ordersFromStream {
		sqlStatement := `INSERT INTO orders (id, data) VALUES ($1, $2)`
		res, err := db.Exec(sqlStatement, v.id, v.data)
		if err != nil {
			log.Println(err)
			continue
		}
		count, err := res.RowsAffected()
		if err != nil {
			log.Println(err)
		}
		fmt.Println("Rows updated: ", count)
	}
	cache := memcache.New(0, 0)
	ordersFromDB := getRowsFromDB(db)
	for _, v := range ordersFromDB {
		cache.Set(v.id, v.data, 0)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/get_order", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		data, ok := cache.Get(id)
		if !ok {
			fmt.Fprintf(w, "искомый заказ с id %v не найден", id)
		}
		s, ok := data.([]byte)
		var str string
		if ok {
			str = string(s)
		}
		fmt.Fprintf(w, "искомый заказ с id %v...\n%v", id, str)
	})

	log.Println("Запуск веб-сервера на http://127.0.0.1:4000")
	errrrr := http.ListenAndServe(":4000", mux)
	log.Fatal(errrrr)
}

func getRowsFromDB(db *sql.DB) []order {
	rows, e := db.Query("select * from orders")
	if e != nil {
		log.Println(e)
	}
	defer rows.Close()
	fmt.Println("Getting orders out of DB...")

	ordersFromDB := []order{}
	for rows.Next() {
		o := order{}
		e := rows.Scan(&o.id, &o.data)
		if e != nil {
			log.Println(e)
			continue
		}
		ordersFromDB = append(ordersFromDB, o)
	}
	return ordersFromDB
}

func getDataFromStream(js nats.JetStreamContext) []order {
	var natsdata [][]byte
	sub, e := js.SubscribeSync("ORDERS.*")
	if e != nil {
		log.Fatal(e)
	}
	queuedMsgs, _, _ := sub.Pending()
	for i := 0; i < queuedMsgs; i++ {
		fmt.Println(queuedMsgs)
		m, err := sub.NextMsg(5 * time.Second)
		if err == nil {
			fmt.Printf("Received a message: %s\n", string(m.Data))
			natsdata = append(natsdata, m.Data)
		} else {
			fmt.Println(err)
			fmt.Println("NextMsg timed out.")
		}
	}

	fmt.Println("Data recieved")
	ordersFromStream := []order{}

	for _, v := range natsdata {
		row := string(v)
		rowArr := strings.Split(row, ",")
		for _, v := range rowArr {
			if strings.Contains(v, "order_uid") {
				tempArr := strings.Split(v, " ")
				tempnumid := tempArr[1]
				Numid = strings.Trim(tempnumid, "\"")
			}
		}
		o := order{}
		o.data = v
		o.id = Numid
		fmt.Println(o.id)
		ordersFromStream = append(ordersFromStream, o)
	}
	return ordersFromStream
}
