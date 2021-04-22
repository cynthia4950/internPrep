package main

import (
    // "time"
    "os"
	"fmt"
	"sync"
	// "github.com/gomodule/redigo/redis"
)

var(
	numFiles int = 11
	numConsumer int = 2
	rmqName string = "demoQueue"
	numSortedFiles int = 0
	intMax int = 2147483647
)

func main() {
    // list := os.Args
	// var mutexProducer sync.Mutex
	os.RemoveAll("data/sorted")
	os.Mkdir("data/sorted", 0755)
	os.RemoveAll("data/output")
	os.Mkdir("data/output", 0755)

	var mutexConsumer sync.Mutex
	for i := 1; i < numFiles; i++{
		fmt.Println("create one producer")
    	producer(i)
	}
	for j := 0; j < numConsumer; j++{
		fmt.Println("create one consumer")
    	consumer(&mutexConsumer)
	}
}