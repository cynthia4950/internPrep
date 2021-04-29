package main

import (
	"reflect"
	"sync"
	"testing"
	// "math"
	"github.com/gomodule/redigo/redis"
	. "github.com/agiledragon/gomonkey"
    . "github.com/smartystreets/goconvey/convey"
)

func Test_produce(t *testing.T) {
	var wg_p sync.WaitGroup
	wg_p.Add(1)
	Convey("TestApplyFunc", t, func() {
		Convey("mock success produce", func() {
			patches := ApplyFunc(readFileAndSend, func(_ int, _ QueueHandler, _ redis.Conn) error {
				return nil
			})
			defer patches.Reset()
			err := produce(&wg_p, 0)
			So(err, ShouldEqual, nil)
		})
	})
}


func Test_readFileAndSend(t *testing.T) {
	var mock_q QueueHandler = &mockQueue{queueName: "mockQueue"}
	var dummyConn redis.Conn
	type args struct {
		i       int
		q       QueueHandler
		connect redis.Conn
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"readFileAndSend",args{0,mock_q,dummyConn},false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := readFileAndSend(tt.args.i, tt.args.q, tt.args.connect); (err != nil) != tt.wantErr {
				t.Errorf("readFileAndSend() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}



func Test_handleAppend_full(t *testing.T) {
	var mock_q QueueHandler = &mockQueue{queueName: "mockQueue"}
	var dummyConn redis.Conn
	fullArr := make([]int, batchSize)
	expectArr := []int{1}
	type args struct {
		nums_arr []int
		num      int
		q        QueueHandler
		connect  redis.Conn
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		// TODO: Add test cases.
		{"test_append_full", args{nums_arr: fullArr, num: 1, q: mock_q, connect: dummyConn}, expectArr},

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handleAppend(tt.args.nums_arr, tt.args.num, tt.args.q, tt.args.connect); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handleAppend() = %v, want %v", got, tt.want)
			}
		})
	}
}


func Test_handleAppend_normal(t *testing.T) {
	var mock_q QueueHandler = &mockQueue{queueName: "mockQueue"}
	var dummyConn redis.Conn
	originArr := []int{1,2,3,4}
	expectArr := []int{1,2,3,4,5}
	type args struct {
		nums_arr []int
		num      int
		q        QueueHandler
		connect  redis.Conn
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		// TODO: Add test cases.
		{"test_append_full", args{nums_arr: originArr, num: 5, q: mock_q, connect: dummyConn}, expectArr},

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handleAppend(tt.args.nums_arr, tt.args.num, tt.args.q, tt.args.connect); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handleAppend() = %v, want %v", got, tt.want)
			}
		})
	}
}
