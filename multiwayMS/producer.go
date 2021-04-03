package main

import (
	"fmt"
	"github.com/adjust/rmq/v3"
	"math/rand"
	"time"
	"strconv"
	"strings"
	"os"
	"bufio"
	"sort"
	"encoding/json"
)

const(
	maxint32 = 2147483647
)

type TaskPayload struct {
	Id     int	`json:"id"`
	Nums	[]int	`json:"nums"`
}


func testFilesGenerator(){
	// generate test files
	for j := 0; j < 11; j++ {
		rand.Seed(time.Now().UnixNano())
		fileName := "data" + strconv.Itoa(j) + ".txt"
		fmt.Println(fileName)
		f, err := os.Create(fileName)

		if err != nil {
			panic(err)
		}

		defer f.Close()
		for i := 0; i < 100000; i++ {
			var r = rand.Intn(maxint32)
			fmt.Fprintf(f, "%d\n", r)
		}

		fmt.Println("done")
	}
}


func SendTask(taskQueue rmq.Queue, payload TaskPayload) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		println(err)
		return
	}
	taskQueue.PublishBytes(bytes)
}



func main() {
	// testFilesGenerator()
	connection, err := rmq.OpenConnection("producer", "tcp", "localhost:6379", 1, nil)
	if err != nil {
		panic(err)
	}
	taskQueue,err := connection.OpenQueue("num_queue")
	if err != nil {
		panic(err)
	}

	for i := 1; i <= 10; i++ {
		
		fileHandle, _ := os.Open("data/data" + strconv.Itoa(i) +".txt")
		defer fileHandle.Close()
		fileScanner := bufio.NewScanner(fileHandle)
		var temp []int
		for fileScanner.Scan() {
			read_line := fileScanner.Text()
			read_line = strings.TrimSuffix(read_line, "\n")
			num, err := strconv.Atoi(read_line)
			if err != nil{
				panic(err)
			}
			temp = append(temp, num)
		}
		sort.Ints(temp)
		
		fmt.Println("send payload with id: ",i)
		/*
		// fmt.Println(temp)
		SendTask(taskQueue, TaskPayload{i, temp})
		*/
		var task = TaskPayload{i,temp}
		taskBytes, err := json.Marshal(task)
		if err != nil {
			panic(err)
		}
		// fmt.Println(taskBytes)
		err = taskQueue.PublishBytes(taskBytes)
	}
	
}


