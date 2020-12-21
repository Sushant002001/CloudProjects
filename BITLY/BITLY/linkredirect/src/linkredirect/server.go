
package main

import (
	"encoding/json"
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
	"github.com/unrolled/render"
	"net/http"
	"log"
	"io/ioutil"
)

var originalLinkDict = make(map[string]string)
var mysql_connect = "bitly:cmpe281@tcp(10.0.3.181:3306)/bitly"
var rabbitmq="amqp://bitly:bitly@10.0.1.123:5672/"
var nosql="internal-NoSQL-cache-lb-1643508470.us-west-2.elb.amazonaws.com"
var url="54.202.241.218:8000/lr"

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
	mx.HandleFunc("/slink/{^[a-zA-Z]{5}$}", blinkHandler(formatter)).Methods("GET")
	mx.HandleFunc("/trendLink", trendHandler(formatter)).Methods("GET")
}

// API Ping Handler
func pingHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		formatter.JSON(w, http.StatusOK, struct{ Test string }{"API is alive!"})
	}
}

func blinkHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var bitlyLink string = req.URL.String()
		bitlyLink=bitlyLink[len(bitlyLink)-5:]
		var originalLink string
		var count int
		var resultLink= map[string]string{}
		
		resp, err := http.Get("http://"+nosql+"/api/"+bitlyLink)
		if err != nil {
			fmt.Println(err, "<-- error || ")
		}else{
			body, err := ioutil.ReadAll(resp.Body)
			json.Unmarshal(body, &resultLink)
     		fmt.Println(resultLink, err)
		}
		if resultLink["OriginalLink"]!=""{
			originalLink = resultLink["OriginalLink"]
			fmt.Println("Cache HIT")
		}else{
			fmt.Println("Cache Miss")
			db, err := sql.Open("mysql", mysql_connect)
			if err != nil {
				log.Fatal(err)
			}else{
				fmt.Println("DB CONN")
			} 

			err = db.QueryRow("select long_link, count_hits from BitlyLinks where short_link=?", bitlyLink).Scan(&originalLink, &count)
			if err != nil {
				log.Fatal(err)
			}
			_, err = db.Exec("Update BitlyLinks Set count_hits = ?  where short_link=?", count+1, bitlyLink)
			if err != nil {
				log.Fatal(err)
			}
		}

		body := "{ \"HashCode\": \""+bitlyLink+"\", \"OriginalLink\": \""+originalLink+"\"}"
	
		queue_send_cache(body)
		consumeQueue()


		log.Println(originalLink)
	  	
		w.Header().Set("Location",originalLink)
	    http.Redirect(w, req, originalLink, 302) 

	    var Link = bitlyLink{
			Link: originalLink,
		}
		formatter.JSON(w, http.StatusOK, Link)
	}
}

func trendHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var TrendingLinks = make(map[string]map[string]string)
		db, err := sql.Open("mysql", mysql_connect)
			if err != nil {
				log.Fatal(err)
			}else{
				fmt.Println("DB CONN")
			} 
		dbQuery := "select short_link,count_hits from BitlyLinks order by count_hits desc limit 10"


	    queryData, err := db.Query(dbQuery)
		if err != nil {
			fmt.Println(err)
		} else {
			for queryData.Next() {
				var shortLink string
				var total_count string
				var minute string 
				var hour string 
				var day string
				_ = queryData.Scan(&shortLink,&total_count)
	    		err = db.QueryRow("select count(short_link) from TrendLinks where short_link='"+shortLink+"' and time > date_sub(now(), interval 1 minute)").Scan(&minute)
		        if err != nil {
		            fmt.Println(err, "<--- error")
		        }
		        fmt.Println(minute)
		        err = db.QueryRow("select count(short_link) from TrendLinks where short_link='"+shortLink+"' and time > date_sub(now(), interval 1 hour)").Scan(&hour)
		        if err != nil {
		            fmt.Println(err, "<--- error")
		        }
		        err = db.QueryRow("select count(short_link) from TrendLinks where short_link='"+shortLink+"' and time > date_sub(now(), interval 1 day)").Scan(&day)
		        if err != nil {
		            fmt.Println(err, "<--- error")
		        }
				TrendingLinks[url+"/slink/"+shortLink] = map[string]string{
				        "Last_1_minute": minute,
				        "Last_1_hour": hour,
				        "Last_1_day": day,
				        "Alltime": total_count,
				    }
				}
			}
			result := map[string]interface{}{
			    "return_value": TrendingLinks,
			}
			formatter.JSON(w, http.StatusOK, result)
		}
}



func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func queue_send_cache(message string) {
	//conn, err := amqp.Dial("amqp://"+rabbitmq_user+":"+rabbitmq_pass+"@"+rabbitmq_server+":"+rabbitmq_port+"/")
	conn, err := amqp.Dial(rabbitmq)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch_cache, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch_cache.Close()

	err = ch_cache.ExchangeDeclare(
            "bitlyExchange_cache", // name
            "direct",      // type
            true,          // durable
            false,         // auto-deleted
            false,         // internal
            false,         // no-wait
            nil,           // arguments
    )
    failOnError(err, "Failed to declare an exchange")

    body := message

    err = ch_cache.Publish(
        "bitlyExchange_cache",         // exchange
        "Bitly_Trend", // routing key
        false, // mandatory
        false, // immediate
        amqp.Publishing{
                ContentType: "application/json",
                Body:        []byte(body),
     	})
    failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

