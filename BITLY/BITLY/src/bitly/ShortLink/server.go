/*
	Gumball API in Go (Version 2)
	Process Order with Go Channels and Mutex
*/

package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	_ "github.com/go-sql-driver/mysql"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
	//"github.com/satori/go.uuid"
	"github.com/unrolled/render"
	"net/http"
	"log"
)

var rabbitmq="amqp://bitly:bitly@10.0.1.123:5672/"

// NewServer configures and returns a Server.
func NewServer() *negroni.Negroni {
	formatter := render.New(render.Options{
		IndentJSON: true,
	})
	n := negroni.Classic()
	mx := mux.NewRouter()
	initRoutes(mx, formatter)
	n.UseHandler(mx)
	return n
}


// API Routes
func initRoutes(mx *mux.Router, formatter *render.Render) {
	mx.HandleFunc("/ping", pingHandler(formatter)).Methods("GET")
	mx.HandleFunc("/link", linkHandler(formatter)).Methods("POST")
}


// API Ping Handler
func pingHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		formatter.JSON(w, http.StatusOK, struct{ Test string }{"API is alive!"})
	}
}


func linkHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {

		//creating a random sequence(shortlink)
		var sLink string = randSeq()
		//decoding the body to get the Link url sent
		decoder := json.NewDecoder(req.Body)
		var m bitlyLink
		err := decoder.Decode(&m)
		if err != nil {
			panic(err)
		}
		
		body := "{ \"HashCode\": \""+sLink+"\", \"OriginalLink\": \""+m.Link+"\"}"
		fmt.Println(body)
		queue_send(body)
		fmt.Println("Published to Exchange BitlyExchange")
		
		//originalLinkDict[sLink] = m.Link
		var Link = bitlyLink{
			Link: "localhost:9090"+ "/slink/" +sLink,
		}
		formatter.JSON(w, http.StatusOK, Link)
	}
}

func randSeq() string {
	var n int = 5
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// Send Order to Exchange for Processing
func queue_send(message string) {
	conn, err := amqp.Dial(rabbitmq)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
            "bitlyExchange", // name
            "direct",      // type
            true,          // durable
            false,         // auto-deleted
            false,         // internal
            false,         // no-wait
            nil,           // arguments
    )
    failOnError(err, "Failed to declare an exchange")
    fmt.Println("Exchange Declared")
    body := message

    err = ch.Publish(
        "bitlyExchange",         // exchange
        "Bitly", // routing key
        false, // mandatory
        false, // immediate
        amqp.Publishing{
                ContentType: "application/json",
                Body:        []byte(body),
     	})
    failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

