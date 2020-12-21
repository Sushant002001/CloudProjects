package main

import (
	"log"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"database/sql"
	"encoding/json"
)

var mysql_connect = "bitly:cmpe281@tcp(10.0.3.181:3306)/bitly"
var rabbitmq="amqp://bitly:bitly@10.0.1.123:5672/"

func main(){
    fmt.Println("I am DB server")
    db, err := sql.Open("mysql", mysql_connect)
    if err != nil {
        log.Fatal(err)
    }else{
        fmt.Println("DB CONN")
    } 
    conn, err := amqp.Dial(rabbitmq)
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()
    consumeQueue(db, conn)

}

func consumeQueue(db *sql.DB, conn *amqp.Connection) {
    
    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()
    fmt.Println("Connection to RabbitMQ and Connection made")
    err = ch.ExchangeDeclare(
            "bitlyExchange",   // name
            "direct", // type
            true,     // durable
            false,    // auto-deleted
            false,    // internal
            false,    // no-wait
            nil,      // arguments
    )
    failOnError(err, "Failed to declare an exchange")
    fmt.Println("Success to declare an exchange bitlyExchange")
    q, err := ch.QueueDeclare(
            "ShortLink",    // name
            false, // durable
            false, // delete when unused
            false,  // exclusive
            false, // no-wait
            nil,   // arguments
    )
    failOnError(err, "Failed to declare a queue")
    fmt.Println("Success to declare a queue ShortLink")
    err = ch.QueueBind(
            q.Name, // queue name
            "Bitly",     // routing key
            "bitlyExchange", // exchange
            false,
            nil,
    )
    failOnError(err, "Failed to bind a queue")
    fmt.Println("Success to bind a queue ShortLink to bitlyExchange")
    msgs1, err := ch.Consume(
            q.Name, // queue
            "database",     // consumer
            true,   // auto ack
            false,  // exclusive
            false,  // no local
            false,  // no wait
            nil,    // args
    )
    failOnError(err, "Failed to register a consumer")
    fmt.Println("Success to register a consumer database ShortLink to bitlyExchange")

    err = ch.ExchangeDeclare(
            "bitlyExchange_cache",   // name
            "direct", // type
            true,     // durable
            false,    // auto-deleted
            false,    // internal
            false,    // no-wait
            nil,      // arguments
    )
    failOnError(err, "Failed to declare an exchange")
     fmt.Println("Success to declare an exchange bitlyExchange_cache")
    q_cache, err := ch.QueueDeclare(
            "cacheHit",    // name
            false, // durable
            false, // delete when unused
            false,  // exclusive
            false, // no-wait
            nil,   // arguments
    )
    failOnError(err, "Failed to declare a queue")
    fmt.Println("Success to declare a queue cacheHit")
    err = ch.QueueBind(
            q_cache.Name, // queue name
            "Bitly_Trend",     // routing key
            "bitlyExchange_cache", // exchange
            false,
            nil,
    )
    failOnError(err, "Failed to bind a queue")
    fmt.Println("queue bind cacheHit to bitlyExchange_cache")
    msgs2, err := ch.Consume(
            q_cache.Name, // queue
            "database_cache",     // consumer
            true,   // auto ack
            false,  // exclusive
            false,  // no local
            false,  // no wait
            nil,    // args
    )
    failOnError(err, "Failed to register a consumer")
    fmt.Println("Success to register a consumer database cacheHit")

    forever := make(chan bool)
        var body1 queueBody
        var body2 queueBody
       go func() { 
                for d := range msgs1 {
                json.Unmarshal([]byte(d.Body), &body1)
                log.Printf("Received a message: %s", d.Body)
                log.Printf("SHORTCODE: %s", body1.HashCode)
                log.Printf("LONGURL: %s", body1.OriginalLink)
                DatabaseEntry(body1.HashCode, body1.OriginalLink, db)
            }
        }()
        

        for d := range msgs2 {
            json.Unmarshal([]byte(d.Body), &body2)
            log.Printf("Received a message: %s", d.Body)
            log.Printf("Received a message: %s", body2.HashCode)
            log.Printf("Received a message: %s", body2.OriginalLink)
            DatabaseUpdate(body2.HashCode, db)
        }
        
    <-forever
}


func DatabaseEntry(sLink string, llink string, db *sql.DB) {
    
    stmt, err := db.Prepare("INSERT INTO BitlyLinks(short_link, long_link, count_hits) VALUES(?,?,0)")
        if err != nil {
            log.Fatal(err)
        }
        if _, err := stmt.Exec(sLink, llink); err != nil {
            log.Fatal(err)
        } 
    defer stmt.Close()
    stmttime, err := db.Prepare("INSERT INTO TrendLinks(short_link, long_link) VALUES(?,?)")
        if err != nil {
            log.Fatal(err)
        }
        if _, err := stmttime.Exec(sLink, llink); err != nil {
            log.Fatal(err)
        }
    fmt.Println("Link inserted Successfully in BitlyLinks")
    defer stmttime.Close()        
}

func DatabaseUpdate(sLink string, db *sql.DB) {
    var count int
    var llink string

    db.QueryRow("select count_hits, long_link from BitlyLinks where short_link=?", sLink).Scan(&count, &llink)
    fmt.Println(count,"<---- count of the link")
    fmt.Println(llink, "<----- associated long URL")
    stmttime, err := db.Prepare("INSERT INTO TrendLinks(short_link, long_link) VALUES(?,?)")
        if err != nil {
            log.Fatal(err)
        }
        if _, err := stmttime.Exec(sLink, llink); err != nil {
            log.Fatal(err)
        }
    fmt.Println("Link inserted Successfully in TrendLinks")
    defer stmttime.Close()  
    _, err = db.Exec("Update BitlyLinks Set count_hits = ?  where short_link=?", count+1, sLink)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Count updated succesully to the BitlyLinks")
}

func failOnError(err error, msg string) {
    if err != nil {
        log.Fatalf("%s: %s", msg, err)
        panic(fmt.Sprintf("%s: %s", msg, err))
    }
}
