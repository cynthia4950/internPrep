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


type SendingManager interface {
	OpenConnAndQueue_Send()rmq.Queue
	SendPayload(rmq.Queue, int) error
}

type RealSend struct {
	port 	string
}

/*
type FakeSend struct {
	connected, sent bool
}
*/



type TaskPayload struct {
	Id     int	`json:"id"`
	Nums	[]int	`json:"nums"`
}



func testFilesGenerator() bool{
	// generate test files
	errorOccurs := false
	for j := 0; j < 11; j++ {
		rand.Seed(time.Now().UnixNano())
		fileName := "../data/data" + strconv.Itoa(j) + ".txt"
		fmt.Println(fileName)
		f, err := os.Create(fileName)

		if err != nil {
			errorOccurs = true
			panic(err)
		}

		defer f.Close()
		for i := 0; i < 100000; i++ {
			var r = rand.Intn(maxint32)
			fmt.Fprintf(f, "%d\n", r)
		}

		fmt.Println("done")
	}
	return errorOccurs
}


func readDataFile(fileName string) []int{
	fileHandle, _ := os.Open(fileName)
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
	return temp
}


func (producer_real *RealSend) OpenConnAndQueue_Send() (rmq.Queue, error){
	connection, err := rmq.OpenConnection("producer", "tcp", "localhost:6379", 1, nil)
	if err != nil {
		// panic(err)
		var emptyQueue rmq.Queue
		return emptyQueue, err
	}

	taskQueue,err := connection.OpenQueue("num_queue")
	if err != nil {
		// panic(err)
		var emptyQueue rmq.Queue
		return emptyQueue, err
	}

	return taskQueue, nil
}


func getContent(payloadId int,fileName string) []byte{
	temp := readDataFile(fileName)
	// fmt.Println("get payload whose id is: ",i)
	// fmt.Println(temp)
	var task = TaskPayload{payloadId,temp}
	taskBytes, err := json.Marshal(task)
	if err != nil {
		panic(err)
	}
	return taskBytes
}

func (producer_real *RealSend) SendPayload(taskQueue rmq.Queue, taskBytes []byte) error{
	err := taskQueue.PublishBytes(taskBytes)
	return err
}



func main() {
	producer_real := RealSend{"localhost:6379"}
	taskQueue,_ := producer_real.OpenConnAndQueue_Send()

	for i := 1; i <= 10; i++ {
		taskBytes := getContent(i, "data/data" + strconv.Itoa(i) +".txt")
		err := producer_real.SendPayload(taskQueue, taskBytes)
		if err != nil {
			panic(err)
		}
	}
	
}


