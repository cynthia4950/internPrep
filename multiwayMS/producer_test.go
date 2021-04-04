package main

import (
	"encoding/json"
	// "testing"
	// . "github.com/agiledragon/gomonkey/v2"
	// . "github.com/smartystreets/goconvey/convey"
	"github.com/adjust/rmq/v3"
)


func SendTask_stub(taskQueue rmq.Queue, payload TaskPayload) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		println(err)
		return
	}
	taskQueue.PublishBytes(bytes)
}

func readDataFile_stub(i int) []int{
	temp := []int {1,2,3,4}
	return temp
}

func openConnAndQueue_stub() rmq.Queue{
	var test_queue rmq.Queue
	return test_queue
}

