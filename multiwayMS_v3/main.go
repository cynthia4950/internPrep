package main

import (
	// "time"
	"fmt"
	"math/rand"
	"os"

	// "rand"
	"strconv"
	"sync"
	"time"
	// "github.com/gomodule/redigo/redis"
)

var (
	numFiles    int    = 11 // numFiles-1 = 实际需文件数
	numConsumer int    = 2
	rmqName     string = "demoQueue"
	intMax      int    = 2147483647
	batchSize          = 25000
)

// Generate data files
func testFilesGenerator() bool {
	// generate test files
	errorOccurs := false
	for j := 1; j < numFiles; j++ {
		rand.Seed(time.Now().UnixNano())
		fileName := "data/data" + strconv.Itoa(j) + ".txt"
		// fmt.Println(fileName)
		f, err := os.Create(fileName)

		if err != nil {
			errorOccurs = true
			panic(err)
		}

		defer f.Close()
		for i := 0; i < 100000; i++ {
			var r = rand.Intn(intMax)
			fmt.Fprintf(f, "%d\n", r)
		}

		// fmt.Println("done")
	}
	fmt.Printf("%v data files generated\n", numFiles-1)
	return errorOccurs
}

func main() {
	// list := os.Args
	// var mutexProducer sync.Mutex

	fmt.Println("Generate data files")
	testFilesGenerator()

	//清理data/sorted 和 data/output底下的文件
	os.RemoveAll("data/sorted")
	os.Mkdir("data/sorted", 0755)
	os.RemoveAll("data/output")
	os.Mkdir("data/output", 0755)

	//使用mutex以免排序后生成的文件的文件名冲突
	//使用wait gourp wg1让消费者在生产者传输完之后再开始消费，
	//wg2防止main在goroutine结束之前就结束
	var mutexConsumer sync.Mutex
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	for i := 1; i < numFiles; i++ {
		fmt.Println("create producer ", i)
		wg1.Add(1)
		go produce(&wg1, i)
	}
	wg1.Wait()

	var q QueueHandler = &Queue{queueName: "demoQueue"}
	for j := 0; j < numConsumer; j++ {
		fmt.Println("create one consumer ", j)
		wg2.Add(1)
		go consume(&wg2, &mutexConsumer, q)
	}
	wg2.Wait()
	fmt.Println("Sort Complete!")
}
