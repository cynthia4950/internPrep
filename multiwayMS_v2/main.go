package main

import (
	"bufio"
	"context"
	// "errors"
    "github.com/gomodule/redigo/redis"
    "os"
    "os/signal"
    "syscall"
	"strconv"
	"fmt"
	"strings"
	"sort"
	"time"
)

var(
	numFiles int
	overFlowMarker []int
)

//读取数据，并对数据进行排序，并传输进消息队列
func readFileAndSend(i int, overFlowMarker []int, q *Queue) error {
	fileName := "data/data"+strconv.Itoa(i)+".txt"
	fileHandle, _ := os.Open(fileName)
	defer fileHandle.Close()
	fileScanner := bufio.NewScanner(fileHandle)
	var temp []int
	for fileScanner.Scan() {
		read_line := fileScanner.Text()
		read_line = strings.TrimSuffix(read_line, "\n")
		num, err := strconv.Atoi(read_line)
		if err != nil {
			panic(err)
		}

		//如果文件太大，正常的append可能不能用：
		temp, overFlowMarker = handleAppend(temp, num, i, overFlowMarker, q)
		
	}

	if len(temp) != 0 {
		sort.Ints(temp)
	}
	msg := &Message{name: "demoQueue", Content: temp}
	q.Delivery(msg)

	return nil
}

func handleAppend(nums_arr []int, num int, i int, overFlowMarker []int, q *Queue)([]int, []int){
	oldLength := len(nums_arr)
	nums_arr = append(nums_arr, num)
	if len(nums_arr) == oldLength {
		//如果文件还未读完就内存告罄， 先把目前的数用消息队列传递出去
		fmt.Println("Fail to append the data read in to temporary storage array. Store the sorted temporary array to another file")
		sort.Ints(nums_arr)
		msg := &Message{name: "demoQueue", Content: nums_arr}
		q.Delivery(msg)

		/*
		f, err := os.Create("data/data" + strconv.Itoa(i)  + "_o" + strconv.Itoa(1+overFlowMarker[i]) + ".txt")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		for j := 0; j < oldLength; j++ {
			fmt.Fprintf(f, "%d\n", nums_arr[j])
		}
		overFlowMarker[i]++
		var emptyArr []int
		return emptyArr, overFlowMarker
		*/

	}
	return nums_arr, overFlowMarker
}


func main() {
	numFiles = 10
	overFlowMarker := make([]int, numFiles)
	for k := 0; k < numFiles; k++{
		overFlowMarker[k] = 0
	}

    pool := &redis.Pool{
        Dial: func() (conn redis.Conn, err error) {
            return redis.Dial("tcp", ":6379")
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            if time.Since(t) < time.Minute {
                return nil
            }
            _, err := c.Do("PING")
            return err
        },
    }
    queue := &Queue{pool: pool}

    msg := &Message{
        name: "demoQueue",
    }

    // make a main context
	ctx := context.Background()
	// pass the main context to queue
	cancelFunc := queue.InitReceiver(ctx, msg, 10)

	
    go func() {
        for i := 0; i < numFiles; i++ {
			//创建被传输的内容，10w个排序好的有序数为一组：
			//"data/data"+strconv.Itoa(i)+".txt"
			err := readFileAndSend(i+1, overFlowMarker, queue)
			if err != nil{
				fmt.Println(err)
			}
			// mergeSort(&nums)
            // msg := &Message{name: "demoQueue", Content: nums}
            // _ = queue.Delivery(msg)
        }
    }()
	

    quit := make(chan os.Signal, 1)

    signal.Notify(quit, syscall.SIGINT)

    for {
        switch <-quit {
        case syscall.SIGINT:
			cancelFunc()
            return
        }
    }
}