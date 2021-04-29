package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
)

/*
生产过程，使用WaitGroup保证一定是先生产完消费者才可以开始获取内容，
一个生产者对应一个文件，因此需要fileIndex
*/
func produce(wg *sync.WaitGroup, fileIndex int) error {
	defer wg.Done()

	redis_conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return err
	}

	defer redis_conn.Close()

	var q QueueHandler = &Queue{queueName: "demoQueue"}

	//创建被传输的内容，10w个未排序好的有序数为一组：
	err = readFileAndSend(fileIndex, q, redis_conn)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

//读取数据，并对数据进行排序，并传输进消息队列
func readFileAndSend(i int, q QueueHandler, connect redis.Conn) error {

	fileName := "data/data" + strconv.Itoa(i) + ".txt"
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

	//传输剩下的、大小不超过batchSize的数组进消息队列
	if len(temp) != 0 {
		msg := Message{queueName: rmqName, Content: temp}
		q.Delivery(msg, connect)
	}

	return nil
}

/*
设定的batchSize保证小于内存，当读取的数组长度到达batchSize，传输数组进消息队列，
如果长度未达到，将读取到的数字加入数组
*/
func handleAppend(nums_arr []int, num int, q QueueHandler, connect redis.Conn) []int {
	var new_nums_arr []int
	if len(nums_arr) == batchSize {
		//如果文件还未读完就内存告罄， 先把目前的数用消息队列传递出去
		// 在确认内存不会溢出的情况下，在这里可以直接用sort.Ints(nums_arr)，但是在这次作业中选择在消费者处用自己写的mergeSort（）
		msg := Message{queueName: rmqName, Content: nums_arr}
		q.Delivery(msg, connect)
		//上一批数字传输后应该只剩下最后加入的那个数
		new_nums_arr = append(new_nums_arr, num)
		return new_nums_arr
	}
	new_nums_arr = append(nums_arr, num)
	return new_nums_arr
}
