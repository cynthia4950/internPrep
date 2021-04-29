package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
)

/*
消费过程，
使用WaitGroup让main等待消费者全部消费完才结束进程
使用mutex使得多个消费者不会同时产生名字一样的sorted文件
所有与消息队列有关的链接、消息发出以及消息接收用QueueHandler interface因为这个作业采用了两种测试方法
*/
func consume(wg *sync.WaitGroup, m *sync.Mutex, q QueueHandler) error {
	defer wg.Done()

	//链接： if statement是为区别测试时的mock queue， mock queue不用关闭连接
	redis_conn := q.Connect()
	if q.GetQueueName() != "mockQueue" {
		defer redis_conn.Close()
	}

	//从队列里接受信息直到接受不到东西为止，当队列为空时，item == nil
	for {
		item, err := q.Retrieve(redis_conn)
		if err != nil {
			err_rewrite := errors.New("err from LPOP")
			return err_rewrite

		} else {
			if item == nil {
				mergeFiles()
				return nil
			}

			//当队列不为空时，可以从队列中得到一个不为空的item，取出item的内容，进行初步排序并放入sorted文件
			//每个item会有自己对应的sorted文件
			itemUint8, ok := item.([]uint8)
			if !ok {
				fmt.Println("!ok: cannot assert reply as type []uint8")
				return errors.New("cannot assert reply as type []uint8")
			}
			var msg Message
			err = json.Unmarshal(itemUint8, &msg)
			// err = json.Unmarshal(item, &msg)
			if err != nil {
				fmt.Println(err)
				return err
			}

			//合并排序：
			msgContent := msg.Content
			mergeSort(&msgContent, 0, len(msgContent)-1)

			//使用mutex产生正确的sorted文件名
			//use mutex to avoid race condition in numSortedFiles
			//output to file called: data/sorted/sorted+numSortedFile+.txt
			m.Lock()
			outputHandle := outputSorted()
			m.Unlock()
			defer outputHandle.Close()

			//把item里的数字逐个写到sorted文件里
			for j := 0; j < len(msgContent); j++ {
				err = writeToFile(outputHandle, msgContent[j])
				if err != nil {
					fmt.Println(err)
				}
			}

		}
	}
}

/*
数文件夹sorted底下有多少个sorted.txt文件，假设数量为n，
创建文件名为sortedn.txt的文件来储存下一次收到的排序数字
*/
func outputSorted() *os.File {
	sortedFiles, _ := ioutil.ReadDir("data/sorted")
	numSortedFiles := len(sortedFiles)
	// fmt.Println("create file sorted" + strconv.Itoa(numSortedFiles) + " to store sorted numbers")
	outputHandle, err := os.Create("data/sorted/sorted" + strconv.Itoa(numSortedFiles) + ".txt")
	if err != nil {
		fmt.Println(err)
	}
	return outputHandle
}

/*
将指定int写入指定文件
*/
func writeToFile(fileHandler *os.File, num int) error {
	_, err := fileHandler.WriteString(fmt.Sprintf("%d\n", num))
	return err
}

/*
合并排序，使用递归与合并merge（）
*/
func mergeSort(arr *[]int, l int, r int) {
	if l < r {
		m := (l + (r - 1)) / 2
		// Sort first and second halves
		mergeSort(arr, l, m)
		mergeSort(arr, m+1, r)
		merge(arr, l, m, r)
	}
}

/*合并*/
func merge(arr *[]int, l int, m int, r int) {
	n1 := m - l + 1
	n2 := r - m

	//create temp arrays
	L := make([]int, n1)
	R := make([]int, n2)

	//Copy data to temp arrays L[] and R[]
	for i := 0; i < n1; i++ {
		L[i] = (*arr)[l+i]
	}

	for j := 0; j < n2; j++ {
		R[j] = (*arr)[m+1+j]
	}

	// Merge the temp arrays back into arr[l..r]
	i := 0 //Initial index of first subarray
	j := 0 //Initial index of second subarray
	k := l //Initial index of merged subarray

	for i < n1 && j < n2 {
		if L[i] <= R[j] {
			(*arr)[k] = L[i]
			i += 1
		} else {
			(*arr)[k] = R[j]
			j += 1
		}
		k += 1
	}
	// Copy the remaining elements of L[], if there
	// are any
	for i < n1 {
		(*arr)[k] = L[i]
		i += 1
		k += 1
	}
	// Copy the remaining elements of R[], if there
	// are any
	for j < n2 {
		(*arr)[k] = R[j]
		j += 1
		k += 1
	}
}

/*
当队列为空时，把已经排序好的小文件们合并成大文件
*/
func mergeFiles() {
	//数有多少个sorted文件，将结果储存在numSortedFiles
	sortedFiles, _ := ioutil.ReadDir("data/sorted")
	numSortedFiles := len(sortedFiles)

	//创建最终结果所要保存在的文件
	outputHandle, err := os.Create("data/output/output.txt")
	if err == nil {
		defer outputHandle.Close()
	}

	//将file scanner储存在scannerArr数组里以方便未来的Scan（）
	scannerArr := make([]*bufio.Scanner, numSortedFiles)
	//将file handler储存在handlerArr数组里方便未来关闭文件
	handlerArr := make([]*os.File, numSortedFiles)

	//对每个sorted file，创建scanner
	for i := 0; i < numSortedFiles; i++ {
		scannerArr, handlerArr = createScanner(i, scannerArr, handlerArr)
	}

	//workingArr作为每个数组里最小数字的暂存点
	workingArr := make([]int, numSortedFiles) //store the number we are working with in each file

	//first iteration, scan first num for all sorted files
	var read_line string
	var num int
	for j := 0; j < numSortedFiles; j++ {
		scannerArr[j].Scan()
		read_line = scannerArr[j].Text()
		read_line = strings.TrimSuffix(read_line, "\n")
		num, _ = strconv.Atoi(read_line)
		workingArr[j] = num
	}

	lastMin := 0             //最小数字所在的文件的数字
	tempMin := workingArr[0] //目前最小的数字

	tempMin, lastMin = findMinAndArr(tempMin, workingArr, numSortedFiles)

	//将第一个最小数写入最终生成文件
	_, err = outputHandle.WriteString(fmt.Sprintf("%d\n", tempMin))
	if err != nil {
		fmt.Println(err)
	}

	//每个循环只扫描上一次产生最小数的文件，将新扫描出来的数放入workingArr，再比较workingArr里数的大小以找出下一次循环中需要扫描的文件
	for {
		scanResult := scannerArr[lastMin].Scan()
		if scanResult {
			// read_line = fileScanner.Text()
			read_line = scannerArr[lastMin].Text()
			read_line = strings.TrimSuffix(read_line, "\n")
			num, _ = strconv.Atoi(read_line)
			workingArr[lastMin] = num
		} else {
			//all nums in this file has been processed
			workingArr[lastMin] = intMax
		}

		tempMin := intMax

		tempMin, lastMin = findMinAndArr(tempMin, workingArr, numSortedFiles)

		//当找到的最小数字仍然是intMax时，所有文件中的数已经被扫描
		if tempMin == intMax {
			//close all files
			for i := 0; i < numSortedFiles; i++ {
				handlerArr[i].Close()
			}
			return
		}

		//output this tempMin to output file
		_, err := outputHandle.WriteString(fmt.Sprintf("%d\n", tempMin))
		if err != nil {
			fmt.Println(err)
		}

	}
}

/*
对每一个sorted文件创建一个scanner，避免所有数都在内存中导致内存溢出
*/
func createScanner(i int, fileScanner_Arr []*bufio.Scanner, fileHandler_Arr []*os.File) ([]*bufio.Scanner, []*os.File) {
	// fmt.Println("create scanner for file with index: ", i)
	fileName := "data/sorted/sorted" + strconv.Itoa(i) + ".txt"
	fh, _ := os.Open(fileName)

	// defer fileHandle.Close()
	fs := bufio.NewScanner(fh)

	fileScanner_Arr[i] = fs
	fileHandler_Arr[i] = fh
	return fileScanner_Arr, fileHandler_Arr
}

/*
找出一个数组中最小的数字以及它的index
==找出最小数字和最小数字所在文件的index
*/
func findMinAndArr(tempMin int, workingArr []int, n int) (int, int) {
	var lastMin int
	for k := 0; k < n; k++ {
		num_working := workingArr[k]
		if tempMin > num_working {
			tempMin = num_working
			lastMin = k
		}
	}
	return tempMin, lastMin
}
