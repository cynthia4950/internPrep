package main

import (
	"bufio"
    "fmt"
    // "time"
    "strconv"
	"strings"
	"sync"
    // "math/rand"
	"os"
    "github.com/gomodule/redigo/redis"
)

// const numFiles int = 10
// const RMQ string = "mqtest"


func produce(wg *sync.WaitGroup, fileIndex int) error {
	defer wg.Done()
	fmt.Println("in producer()")

    redis_conn, err := redis.Dial("tcp", ":6379")
    if err != nil {
        fmt.Println(err)
        return err
    }
    
    defer redis_conn.Close()
    
    // rand.Seed(time.Now().UnixNano())

	var q QueueHandler = &Queue{queueName: "demoQueue"}

	//创建被传输的内容，10w个未排序好的有序数为一组：
	fmt.Println("call readFileAndSend()")
	err = readFileAndSend(fileIndex, q, redis_conn)
	if err != nil{
		fmt.Println(err)
		return err
	}


	return nil
}


//读取数据，并对数据进行排序，并传输进消息队列
func readFileAndSend(i int, q QueueHandler, connect redis.Conn) error {

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
		temp = handleAppend(temp, num, q, connect)
	}

	if len(temp) != 0 {
		msg := Message{queueName: rmqName, Content: temp}
		fmt.Println("call q.Delivery in readFileAndSend")
		q.Delivery(msg, connect)
	}

	return nil
}

func handleAppend(nums_arr []int, num int, q QueueHandler, connect redis.Conn) []int{
	var new_nums_arr []int
	if len(nums_arr) == batchSize {
		//如果文件还未读完就内存告罄， 先把目前的数用消息队列传递出去
		fmt.Println("Fail to append the data read in to temporary storage array. Store the sorted temporary array to another file")
		// 在确认内存不会溢出的情况下，在这里可以直接用sort，但是在这次作业中选择在消费者处用自己写的mergeSort（）
		// sort.Ints(nums_arr)
		
		msg := Message{queueName: rmqName, Content: nums_arr}
		fmt.Println("call q.Delivery in handleAppend")
		q.Delivery(msg, connect)

		//上一批数字传输后应该只剩下最后加入的那个数
		new_nums_arr = append(new_nums_arr, num)
		return new_nums_arr
	}
	new_nums_arr = append(nums_arr, num)
	return new_nums_arr
}