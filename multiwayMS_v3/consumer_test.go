package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	. "github.com/agiledragon/gomonkey"
	"github.com/gomodule/redigo/redis"
	. "github.com/smartystreets/goconvey/convey"
)

/*
内联会使测试产生问题 因此使用：-v -gcflags=-l
go test -v -gcflags=-l -coverprofile=coverage.out
*/

func Test_writeToFile(t *testing.T) {
	num_in := 7
	fileHandler_in, _ := os.Create("data/data1.txt")
	defer fileHandler_in.Close()

	type args struct {
		fileHandler *os.File
		num         int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"write a number to a file", args{fileHandler: fileHandler_in, num: num_in}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := writeToFile(tt.args.fileHandler, tt.args.num); (err != nil) != tt.wantErr {
				t.Errorf("writeToFile() error = %v, wantErr %v", err, tt.wantErr)

				var num_out int
				fileHandle_out, _ := os.Open("data/data1.txt")
				defer fileHandle_out.Close()
				fileScanner_out := bufio.NewScanner(fileHandle_out)

				for {
					scanResult := fileScanner_out.Scan()
					if scanResult {
						read_line := fileScanner_out.Text()
						read_line = strings.TrimSuffix(read_line, "\n")
						num_out, _ = strconv.Atoi(read_line)
					} else {
						break
					}
				}

				if num_out != num_in {
					t.Errorf("writeToFile() error = %v, wantErr %v", num_in, num_out)
				}

			}
		})
	}
}

//checked
func Test_mergeSort(t *testing.T) {
	type args struct {
		arr *[]int
		l   int
		r   int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"test_mergeSort", args{arr: &[]int{2, 5, 7, 2, 4, 6, 9, 0}, l: 0, r: 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mergeSort(tt.args.arr, tt.args.l, tt.args.r)
			expected := []int{0, 2, 2, 4, 5, 6, 7, 9}
			if !reflect.DeepEqual(expected, *tt.args.arr) {
				// t.Errorf("mergeSort fails")
				t.Errorf("want: %v, got: %v", expected, tt.args.arr)
			}
		})
	}
}

func Test_mergeFiles(t *testing.T) {
	os.RemoveAll("data/sorted")
	os.Mkdir("data/sorted", 0755)
	os.RemoveAll("data/output")
	os.Mkdir("data/output", 0755)

	content1 := []int{0, 2, 2, 4, 5, 6, 7, 9}
	content2 := []int{9, 11, 23, 29}
	content3 := []int{0, 1, 2, 4}
	output_expect := []int{0, 0, 1, 2, 2, 2, 4, 4, 5, 6, 7, 9, 9, 11, 23, 29}

	fileHandler1, _ := os.Create("data/sorted/sorted0.txt")
	fileHandler2, _ := os.Create("data/sorted/sorted1.txt")
	fileHandler3, _ := os.Create("data/sorted/sorted2.txt")
	defer fileHandler1.Close()
	defer fileHandler2.Close()
	defer fileHandler3.Close()

	for i := 0; i < len(content1); i++ {
		fileHandler1.WriteString(fmt.Sprintf("%d\n", content1[i]))
	}
	for i := 0; i < len(content2); i++ {
		fileHandler2.WriteString(fmt.Sprintf("%d\n", content2[i]))
	}
	for i := 0; i < len(content3); i++ {
		fileHandler3.WriteString(fmt.Sprintf("%d\n", content3[i]))
	}

	tests := []struct {
		name string
	}{
		// TODO: Add test cases.
		{"test merge 3 files"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mergeFiles()
			var output_real []int

			fileName := "data/output/output.txt"
			fileHandle, _ := os.Open(fileName)
			defer fileHandle.Close()
			fileScanner := bufio.NewScanner(fileHandle)

			for {
				scanResult := fileScanner.Scan()
				if scanResult {
					read_line := fileScanner.Text()
					read_line = strings.TrimSuffix(read_line, "\n")
					num, _ := strconv.Atoi(read_line)
					output_real = append(output_real, num)
				} else {
					break
				}
			}

			if !reflect.DeepEqual(output_expect, output_real) {
				// t.Errorf("mergeSort fails")
				t.Errorf("want: %v, got: %v", output_expect, output_real)
			}

		})
	}
}

func Test_consume(t *testing.T) {
	e := &Queue{queueName: "mockQueue"}
	Convey("TestApplyMethodSeq", t, func() {
		Convey("retrieve returns a valid item", func() {
			//为q.Connet打桩
			patches := ApplyMethod(reflect.TypeOf(e), "Connect", func(_ *Queue) redis.Conn {
				var dummyConn redis.Conn
				return dummyConn
			})
			defer patches.Reset()

			//为q.Retrieve打桩
			dummy_msg := Message{queueName: "dummy", Content: []int{0, 1, 2, 3, 4}}
			item_fake, _ := json.Marshal(dummy_msg)
			var msg_interface interface{} = item_fake
			outputs := []OutputCell{
				{Values: Params{msg_interface, nil}},
				{Values: Params{nil, nil}},
			}
			ApplyMethodSeq(reflect.TypeOf(e), "Retrieve", outputs)

			//打桩 mergeSort
			ApplyFunc(mergeSort, func(_ *[]int, _ int, _ int) {
			})

			//打桩 mergeFiles
			ApplyFunc(mergeFiles, func() {
			})

			//打桩 outputSorted
			ApplyFunc(outputSorted, func() *os.File {
				var dummyFile os.File
				return &dummyFile
			})

			//打桩 writeToFile
			ApplyFunc(writeToFile, func(_ *os.File, _ int) error {
				return nil
			})

			var mutex sync.Mutex
			var wg_c sync.WaitGroup
			wg_c.Add(1)

			err := consume(&wg_c, &mutex, e)
			So(err, ShouldEqual, nil)
		})

	})
}
