/*
	Gumball API in Go (Version 2)
	Process Order with Go Channels and Mutex
*/

package main

import "sync"

type gumballMachine struct {
	Id            int
	CountGumballs int
	ModelNumber   string
	SerialNumber  string
}

var machine gumballMachine = gumballMachine{
	Id:            1,
	CountGumballs: 900,
	ModelNumber:   "M102988",
	SerialNumber:  "1234998871109",
}

type queueBody struct{
	HashCode  string
	OriginalLink  string
}

type bitlyLink struct {
	Link   string
}

type stat struct{
	  Last1minute string `json:"last1minute"`
      Last1hour string `json:"last1hour"`
      Last1day string `json:"last1day"`
      Alltime string `json:"Alltime"`
}
/*type originalLink struct{
	oLink  string
}*/

type order struct {
	OrderStatus string
}

var mutex = &sync.Mutex{}
var orders map[string]order
var order_queue = make(chan string)
