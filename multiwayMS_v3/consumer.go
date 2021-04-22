package main

import (
	"bufio"
	"encoding/json"
	"errors"
    "fmt"
	"strconv"
	"strings"
	"sync"
	"os"
	"io/ioutil"
    "github.com/gomodule/redigo/redis"
)

func consumer(m *sync.Mutex) error {
	fmt.Println("in consumer()")

    redis_conn, err := redis.Dial("tcp", ":6379")
    if err != nil {
        fmt.Println(err)
        return err
    } 
    
    defer redis_conn.Close()

    for {
        item,err := redis_conn.Do("LPOP", rmqName)
		fmt.Println("after LPOP")
		fmt.Println(err)
        if err != nil {
			fmt.Println("err from LPOP")
			return err
            
        } else {
            //consume items:
			fmt.Println("no error from LPOP, start consume items")
			// fmt.Println(item)
			if item == nil {
				fmt.Println("no msg. start to merge files.")
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
			if err != nil {
				fmt.Println(err)
				return err
			}
			// fmt.Println("call mergeSort()")
			msgContent := msg.Content
			mergeSort(&msgContent, 0, len(msgContent)-1)

			//use mutex to avoid race condition in numSortedFiles
			//output to file called: data/sorted/sorted+numSortedFile+.txt
			m.Lock()
			sortedFiles,_ := ioutil.ReadDir("data/sorted")
			numSortedFiles := len(sortedFiles)
			fmt.Println("create file sorted" + strconv.Itoa(numSortedFiles) + " to store sorted numbers")
			outputHandle, err := os.Create("data/sorted/sorted" + strconv.Itoa(numSortedFiles) + ".txt")
			m.Unlock()

			if err == nil {
				defer outputHandle.Close()
			}
			for j := 0; j < len(msgContent); j++ {
				_, err := outputHandle.WriteString(fmt.Sprintf("%d\n", msgContent[j]))
				if err != nil {
					fmt.Println(err)
				}
			}

			sortedFiles,_ = ioutil.ReadDir("data/sorted")
			numSortedFiles = len(sortedFiles)
			fmt.Println("numSortedFile: " + strconv.Itoa(numSortedFiles))
			
        }
    }
}

func mergeSort(arr *[]int, l int, r int){
    if l < r {
        m := (l+(r-1))/2
        // Sort first and second halves
        mergeSort(arr, l, m)
        mergeSort(arr, m+1, r)
        merge(arr, l, m, r)
	}
}

func merge(arr *[]int, l int, m int, r int){
    n1 := m - l + 1
    n2 := r- m
  
    //create temp arrays
    L := make([]int,n1)
    R := make([]int,n2)
  
    //Copy data to temp arrays L[] and R[]
    for i := 0; i < n1; i++{
        L[i] = (*arr)[l + i]
	}
  
    for j := 0; j < n2; j++{
        R[j] = (*arr)[m + 1 + j]
	}
  
    // Merge the temp arrays back into arr[l..r]
    i := 0     //Initial index of first subarray
    j := 0     //Initial index of second subarray
    k := l     //Initial index of merged subarray
  
    for i < n1 && j < n2 {
        if L[i] <= R[j]{
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

func mergeFiles(){
	sortedFiles,_ := ioutil.ReadDir("data/sorted")
	numSortedFiles := len(sortedFiles)

	fmt.Println("in mergeFiles(), numSortedFile is: " + strconv.Itoa(numSortedFiles))
	//for output file:
	outputHandle, err := os.Create("data/output/output.txt")
	if err == nil {
		defer outputHandle.Close()
	}
	scannerArr := make([]*bufio.Scanner, numSortedFiles)
	//create "numSortedFiles" file scanner
	for i := 0; i < numSortedFiles; i++{
		scanner_i,file_i := createScanner(i)
		scannerArr[i] = scanner_i
		defer file_i.Close()
	}
	fmt.Println("mergeFiles: before for loop, numSortedFiles: " + strconv.Itoa(numSortedFiles))
	
	//first iteration, scan first num for all sorted files
	workingArr := make([]int, numSortedFiles)//store the number we are working with in each file
	lastMin := 0
	tempMin := intMax
	for j := 0; j < numSortedFiles; j++{
		fileScanner := scannerArr[j]
		fileScanner.Scan()
		read_line := fileScanner.Text()
		read_line = strings.TrimSuffix(read_line, "\n")
		num,_ := strconv.Atoi(read_line)
		workingArr[j] = num
	}
	for k := 0; k < numSortedFiles; k++{
		num := workingArr[k]
		if tempMin > num {
			tempMin = num
			lastMin = k
		}
	}

	_, err = outputHandle.WriteString(fmt.Sprintf("%d\n", tempMin))
	if err != nil {
		fmt.Println(err)
	}
	
	for {
		// for j := 0; j < numSortedFiles; j++{
			// if j == lastMin{
		fileScanner := scannerArr[lastMin]
		scanResult := fileScanner.Scan()
		if scanResult {
			read_line := fileScanner.Text()
			read_line = strings.TrimSuffix(read_line, "\n")
			num,_ := strconv.Atoi(read_line)
			workingArr[lastMin] = num
		} else {
			//all nums in this file has been processed
			workingArr[lastMin] = intMax
		}
			// }
		// }
		//find min from workingArr
		tempMin := intMax
		for k := 0; k < numSortedFiles; k++{
			num := workingArr[k]
			if tempMin > num {
				tempMin = num
				lastMin = k
			}
		}

		if tempMin == intMax {
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

func createScanner(i int) (*bufio.Scanner, *os.File){
	fileName := "data/sorted/sorted"+strconv.Itoa(i)+".txt"
	fileHandle, _ := os.Open(fileName)
	// defer fileHandle.Close()
	fileScanner := bufio.NewScanner(fileHandle)
	return fileScanner,fileHandle

}