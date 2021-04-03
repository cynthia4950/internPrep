package main

import (
	"fmt"
	"bufio"
	"os"
	"os/signal"
	"syscall"
	"time"
	"strconv"
	"encoding/json"
	"github.com/adjust/rmq/v3"
)

const (
	prefetchLimit = 1
	pollDuration  = 100 * time.Millisecond
	numConsumers  = 1
	inputSize = 100000
	numArrays = 10
	maxint32 = 2147483647
	consumeDuration = time.Millisecond

)

type Batch struct {
	Id     int	`json:"id"`
	Nums	[]int	`json:"nums"`
}

type Consumer struct {
	id  int
}

var allNums [][]int
var indexPtrs [10]int
var complete [10]bool
var allComplete bool


func NewConsumer(tag int) *Consumer {
	return &Consumer{
		id: 	tag,
	}
}

/*
func initializer () ([10]bool, bool){
	for i := 0; i < 10; i++ {
		complete[i] = false
	}
	// count = 0
	allComplete = false
	return complete, allComplete
}
*/

func merge(allNums [][]int) []int{
	fmt.Println("in merge")
	
	var res []int
	var workingArr []int
	var workingIndex int
	var workingNum int
	fileHandle, err := os.Create("data/output.txt")
	if err != nil {
		panic(err)
	}
	defer fileHandle.Close()
	fileWriter := bufio.NewWriter(fileHandle)


	
	minArr := 0	//the index of the array whose number pointed by the pointer is the minimum at this turn
	// test_count := 0
	for !allComplete {
		tempMin := maxint32
		for i := 0; i < numArrays; i++{
			if complete[i] {
				// fmt.Println("skip the array at index " + strconv.Itoa(minArr))
				continue;
			}
			//workingArr is the array we are currently checking it's next number
			workingArr = allNums[i]
			// fmt.Println("working arr size: " + strconv.Itoa(len(workingArr)) )

			//workingIndex is the index of the next number we consider to merge
			workingIndex = indexPtrs[i]
			// fmt.Println("working index: " + strconv.Itoa(workingIndex) )

			//the next number
			workingNum = workingArr[workingIndex]

			if(workingNum <= tempMin){
				tempMin = workingNum
				minArr = i
			}
		}
		indexPtrs[minArr]++;
		if indexPtrs[minArr] >= inputSize {
			complete[minArr] = true
			allComplete = true
			for j := 0; j < numArrays; j++ {
				if !complete[j] {
					allComplete = false
				}
			}
		}
		res = append(res, tempMin)
		_, err = fileWriter.WriteString(fmt.Sprintf("%d\n", tempMin))
        if err != nil {
            fmt.Printf("error writing string: %v", err)
        }
	}


	fmt.Println("end merge")
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

func append_payload(task Batch) [][]int{
	allNums = append(allNums,task.Nums)
	return allNums
}

func mergeTenBatches() []int{
	var res []int
	if len(allNums) == numArrays {
		res = merge(allNums)
	}
	return res
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	// fmt.Println("In Comsume(), id: " + strconv.Itoa(consumer.id))
	/*
	var task Batch
    if err := json.Unmarshal([]byte(delivery.Payload()), &task); err != nil {
        // handle json error
        if err := delivery.Reject(); err != nil {
            // handle reject error
			fmt.Println(err)
        }
		return
    }
	*/
	task := unmarshallPayload(delivery)

	// fmt.Println("task.id: " + strconv.Itoa(task.Id)) 
	// fmt.Println("task.Nums size: " + strconv.Itoa(len(task.Nums)))
	
	// allNums = append(allNums,task.Nums)
	append_payload(task)

    // perform task
    if err := delivery.Ack(); err != nil {
        // handle ack error
		fmt.Println(err)
    }

	// fmt.Println("end consume")
	// fmt.Println("len(allNums): " + strconv.Itoa(len(allNums)))
	
	// if len(allNums) == numArrays {
	// 	merge()
	// }
	mergeTenBatches()
	

}



func main() {
	// fmt.Println("in comsumer main")
	errChan := make(chan error, 10)
	// go logErrors(errChan)

	connection, err := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 1, errChan)
	// defer connection.Close()
	if err != nil {
		panic(err)
	}
	// fmt.Println("in comsumer main, after connection")


	queue, err := connection.OpenQueue("num_queue")
	if err != nil {
		panic(err)
	}
	// fmt.Println("in comsumer main, after open queue")


	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		panic(err)
	}

	// for i := 0; i < numConsumers; i++ {
		fmt.Println("create consumer with id: " + strconv.Itoa(0))
	
		if _, err := queue.AddConsumer("consumer", NewConsumer(0)); err != nil {
			panic(err)
		}
	// }
	fmt.Println("in comsumer main, after add consumer")
	
	
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	<-signals // wait for signal
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()
	<-connection.StopAllConsuming() // wait for all Consume() calls to finish
	

}