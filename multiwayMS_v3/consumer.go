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

func consume(wg *sync.WaitGroup, m *sync.Mutex, q QueueHandler) error {
	defer wg.Done()

	redis_conn := q.Connect()
	if q.GetQueueName() != "mockQueue" {
		defer redis_conn.Close()
	}

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
			// fmt.Println("call mergeSort()")
			msgContent := msg.Content
			// fmt.Println("size of arr before merge sort: " + strconv.Itoa(len(msgContent)))
			mergeSort(&msgContent, 0, len(msgContent)-1)
			// fmt.Println("size of arr after merge sort: " + strconv.Itoa(len(msgContent)))

			//use mutex to avoid race condition in numSortedFiles
			//output to file called: data/sorted/sorted+numSortedFile+.txt
			m.Lock()
			outputHandle := outputSorted()
			m.Unlock()

			if err == nil {
				defer outputHandle.Close()
			}
			for j := 0; j < len(msgContent); j++ {
				// _, err := outputHandle.WriteString(fmt.Sprintf("%d\n", msgContent[j]))
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

func writeToFile(fileHandler *os.File, num int) error {
	_, err := fileHandler.WriteString(fmt.Sprintf("%d\n", num))
	return err
}

func mergeSort(arr *[]int, l int, r int) {
	if l < r {
		m := (l + (r - 1)) / 2
		// Sort first and second halves
		mergeSort(arr, l, m)
		mergeSort(arr, m+1, r)
		merge(arr, l, m, r)
	}
}

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

func mergeFiles() {
	sortedFiles, _ := ioutil.ReadDir("data/sorted")
	numSortedFiles := len(sortedFiles)

	// fmt.Println("in mergeFiles(), numSortedFile is: " + strconv.Itoa(numSortedFiles))
	//for output file:
	outputHandle, err := os.Create("data/output/output.txt")
	if err == nil {
		defer outputHandle.Close()
	}
	scannerArr := make([]*bufio.Scanner, numSortedFiles)
	handlerArr := make([]*os.File, numSortedFiles)
	//create "numSortedFiles" file scanner
	for i := 0; i < numSortedFiles; i++ {
		scannerArr, handlerArr = createScanner(i, scannerArr, handlerArr)
	}
	// fmt.Println("mergeFiles: before for loop, numSortedFiles: " + strconv.Itoa(numSortedFiles))

	//first iteration, scan first num for all sorted files
	workingArr := make([]int, numSortedFiles) //store the number we are working with in each file

	// var fileScanner *bufio.Scanner
	var read_line string
	var num int
	for j := 0; j < numSortedFiles; j++ {
		scannerArr[j].Scan()
		read_line = scannerArr[j].Text()
		read_line = strings.TrimSuffix(read_line, "\n")
		num, _ = strconv.Atoi(read_line)
		// fmt.Println("j: ", j)
		// fmt.Println("文件指针对应的数字: ", num)
		workingArr[j] = num
	}

	lastMin := 0
	tempMin := workingArr[0]

	for k := 0; k < numSortedFiles; k++ {
		num_working := workingArr[k]
		if tempMin > num_working {
			tempMin = num_working //目前最小的数字
			lastMin = k           //最小数字所在的文件的数字
		}
	}

	// fmt.Println("1:目前最小数字: ", tempMin)
	// fmt.Println("1:目前最小数字的来源：", lastMin)

	_, err = outputHandle.WriteString(fmt.Sprintf("%d\n", tempMin))
	if err != nil {
		fmt.Println(err)
	}

	for {
		// fmt.Println("从文件i扫描下一个数： ", lastMin)
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
		for k := 0; k < numSortedFiles; k++ {
			num_working := workingArr[k]
			if tempMin > num_working {
				tempMin = num_working
				lastMin = k
			}
		}

		// fmt.Println("目前最小数字：", tempMin)
		// fmt.Println("目前最小数字的来源：", lastMin)

		if tempMin == intMax {
			//close all files
			for i := 0; i < numSortedFiles; i++ {
				handlerArr[i].Close()
			}
			return
		}
		//output this tempMin to output file,
		_, err := outputHandle.WriteString(fmt.Sprintf("%d\n", tempMin))
		if err != nil {
			fmt.Println(err)
		}

	}
	//scan each file line by line and compare

}

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
