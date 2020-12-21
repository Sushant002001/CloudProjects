package main

import (
    "log"
    "fmt"
    "bytes"
    //_ "github.com/go-sql-driver/mysql"
    //"github.com/codegangsta/negroni"
    "github.com/streadway/amqp"
    //"database/sql"
    "encoding/json"
    "net/http"
    "time"
)
func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
        }
}

func main(){
    go consumeQueue()
    //go consumeQueueCache()
    for true {
        time.Sleep(10*time.Second)
    }
}

func consumeQueue() {
	for true{
        conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
            failOnError(err, "Failed to connect to RabbitMQ")
            defer conn.Close()

        ch, err := conn.Channel()
        failOnError(err, "Failed to open a channel")
        defer ch.Close()
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
        fmt.Println("exchange declare")
        q, err := ch.QueueDeclare(
                "ShortLinkCache",    // name
                false, // durable
                false, // delete when unused
                false,  // exclusive
                false, // no-wait
                nil,   // arguments
        )
        failOnError(err, "Failed to declare a queue")
        fmt.Println("queue declare")
        err = ch.QueueBind(
                q.Name, // queue name
                "Bitly",     // routing key
                "bitlyExchange", // exchange
                false,
                nil,
        )
        failOnError(err, "Failed to bind a queue")
        fmt.Println("queue bind")
        msgs, err := ch.Consume(
                q.Name, // queue
                "cache",     // consumer
                true,   // auto ack
                false,  // exclusive
                false,  // no local
                false,  // no wait
                nil,    // args
        )
        failOnError(err, "Failed to register a consumer")
        var bodycache queueBody
        for d := range msgs {
            json.Unmarshal([]byte(d.Body), &bodycache)
            log.Printf("Received a message: %s", d.Body)
            break
        }
        CacheEntry(bodycache.HashCode, bodycache.OriginalLink)
    }
}

func CacheEntry(sLink string, llink string) {
    message := map[string]interface{}{
       "OriginalLink": llink,
    }

    bytesRepresentation, err := json.Marshal(message)
    if err != nil {
        log.Fatalln(err)
    }

    resp, err := http.Post("http://localhost:9001/api/"+sLink, "application/json", bytes.NewBuffer(bytesRepresentation))
    if err != nil {
        log.Fatalln(err)
    }

    var result map[string]interface{}

    json.NewDecoder(resp.Body).Decode(&result)

    log.Println(result)
    log.Println(result["data"])
}