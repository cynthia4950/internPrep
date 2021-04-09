package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/adjust/rmq/v3"
)

const (
	prefetchLimit   = 1
	pollDuration    = 100 * time.Millisecond
	numConsumers    = 1
	inputSize       = 100000
	numArrays       = 10
	maxint32        = 2147483647
	consumeDuration = time.Millisecond
)

type ReceivingManager interface {
	OpenConnAndProcess()
}

type RealReceive struct {
	port string
}

/*
type FakeReceive struct {
	connected, received, consumed bool
}
*/

type Batch struct {
	Id   int   `json:"id"`
	Nums []int `json:"nums"`
}

type Consumer struct {
	id int
}

type fakeConsumer struct {
	id   int
	fake bool
}

//不要使用全局变量，多协程+多线程同时操作时，会出现coredump或者内存混乱
//变量的作用域越小，代码越清晰易懂
var allNums [][]int
var indexPtrs []int
var complete []bool
var allComplete bool

func NewConsumer(tag int) *Consumer {
	return &Consumer{
		id: tag,
	}
}

func NewFakeConsumer(tag int) *fakeConsumer {
	return &fakeConsumer{
		id: tag,
	}
}

//store直接作为返回值即可，考虑func createOutputFile(fileName string) （store *os.File err error)
func createOutputFile(store **os.File, fileName string) bool {
	temp, err := os.Create(fileName)
	if err != nil {
		panic(err) //正式程序，不要有任何panic的行为，返回error即可
	}
	*store = temp
	return true
}

//complete *[]bool用法不对，数组本身是传址，不需要指针。后面的参数同理。
func findMinNum(complete *[]bool, allNums *[][]int, indexPtrs *[]int, row_size int) (int, int) {
	tempMin := maxint32
	minArr := 0

	//go的数组不是C的数组，实际上用len(complete)就能获取
	for i := 0; i < row_size; i++ {
		if (*complete)[i] {
			// fmt.Println("skip the array at index " + strconv.Itoa(minArr))
			continue
		}
		//workingArr is the array we are currently checking it's next number
		workingArr := (*allNums)[i]
		// fmt.Println("lenth of workingArr:" + strconv.Itoa(len(workingArr)))

		//workingIndex is the index of the next number we consider to merge
		workingIndex := (*indexPtrs)[i]
		// fmt.Println("working index" + strconv.Itoa(workingIndex))

		//the next number
		workingNum := workingArr[workingIndex]

		if workingNum <= tempMin {
			tempMin = workingNum
			minArr = i
		}
	}

	return tempMin, minArr
}

//allComplete作为入参没有用，直接作为返回值即可
func checkCompleArr(minArr int, colSize int, indexPtrs *[]int, complete *[]bool, allComplete *bool) bool {
	//minArr is the index of the array whose number being selected is the minunum
	//complete is an aray of boolean marking which array has been finished
	//allComplete mark if all arrays are finished

	(*indexPtrs)[minArr]++
	if (*indexPtrs)[minArr] >= colSize {
		(*complete)[minArr] = true
		*allComplete = true
		for j := 0; j < len(*complete); j++ {
			if !(*complete)[j] {
				*allComplete = false
			}
		}
	}

	return *allComplete
}

func writeToFile(fileHandle *os.File, content int) bool {
	_, err := fileHandle.WriteString(fmt.Sprintf("%d\n", content))
	if err != nil {
		fmt.Println(err)
	}
	return true
}

//numArrays 全局常量和参数重名，易产生bug
func merge(complete []bool, allNums [][]int, indexPtrs []int, numArrays int, inputSize int) []int {
	fmt.Println("in merge")

	var res []int
	var fileHandle *os.File
	// var fileHandle *os.File
	createSuccess := createOutputFile(&fileHandle, "data/output.txt")
	if createSuccess {
		defer fileHandle.Close()
	}

	// minArr: the index of the array whose number pointed by the pointer is the minimum at this turn
	// test_count := 0
	allComplete = false
	for !allComplete {
		tempMin, minArr := findMinNum(&complete, &allNums, &indexPtrs, numArrays)
		checkCompleArr(minArr, inputSize, &indexPtrs, &complete, &allComplete)
		res = append(res, tempMin)
		writeToFile(fileHandle, tempMin)
	}

	fmt.Println("end merge: Ten payloads have been consumed")
	return res

}

func unmarshallPayload(delivery rmq.Delivery) Batch {
	var task Batch
	if err := json.Unmarshal([]byte(delivery.Payload()), &task); err != nil {
		// handle json error
		if err := delivery.Reject(); err != nil {
			// handle reject error
			fmt.Println(err)
		}
	}
	return task
}

func append_payload(task Batch, original *[][]int) [][]int {
	//多线程+协程场景，会有问题
	*original = append(*original, task.Nums)
	return *original
}

func mergeTenBatches(allNums [][]int, numArrays int) []int {
	var res []int
	if len(allNums) == numArrays {
		res = merge(complete, allNums, indexPtrs, numArrays, inputSize)
	}
	return res
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	// fmt.Println("In Comsume(), id: " + strconv.Itoa(consumer.id))
	task := unmarshallPayload(delivery)

	// fmt.Println("task.id: " + strconv.Itoa(task.Id))
	// fmt.Println("task.Nums size: " + strconv.Itoa(len(task.Nums)))
	append_payload(task, &allNums)

	// perform task
	if err := delivery.Ack(); err != nil {
		// handle ack error
		fmt.Println(err)
	}

	mergeTenBatches(allNums, numArrays)
}

func (consumer *fakeConsumer) Consume(delivery rmq.Delivery, fake_allNums *[][]int, fake_numArrays *int) {
	// fmt.Println("In Comsume(), id: " + strconv.Itoa(consumer.id))
	task := unmarshallPayload(delivery)

	// fmt.Println("task.id: " + strconv.Itoa(task.Id))
	// fmt.Println("task.Nums size: " + strconv.Itoa(len(task.Nums)))
	append_payload(task, fake_allNums)

	mergeTenBatches(*fake_allNums, *fake_numArrays)
}

//考虑函数原型 func (consumer_real *RealReceive) OpenConnAndProcess() (err error)
func (consumer_real *RealReceive) OpenConnAndProcess() {
	errChan := make(chan error, 10)
	// go logErrors(errChan)

	connection, err := rmq.OpenConnection("consumer", "tcp", consumer_real.port, 1, errChan)
	// defer connection.Close()
	if err != nil {
		panic(err) //不要panic
	}

	queue, err := connection.OpenQueue("num_queue")
	if err != nil {
		panic(err)
	}

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		panic(err)
	}

	fmt.Println("create consumer with id: " + strconv.Itoa(0))

	if _, err := queue.AddConsumer("consumer", NewConsumer(0)); err != nil {
		panic(err)
	}

	fmt.Println("in comsumer main, after add consumer")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	<-signals // wait for signal
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()
	// queue.StopConsuming()
	<-connection.StopAllConsuming() // wait for all Consume() calls to finish

}

func main() {
	// fmt.Println("in comsumer main")
	indexPtrs = make([]int, 10)
	complete = make([]bool, 10)
	consumer_real := RealReceive{"localhost:6379"}
	consumer_real.OpenConnAndProcess()
}
