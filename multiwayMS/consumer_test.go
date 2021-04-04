package main

import (
	"encoding/json"
	"testing"
	. "github.com/agiledragon/gomonkey"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/adjust/rmq/v3"
	// "github.com/golang/mock/gomock"
	// "multiwayMS/mock"
)

func unmarshallPayload_stub(delivery rmq.Delivery) Batch {
	temp_arr := []int{0,1,2,3}
	var fake_task_batch = Batch{1,temp_arr}
	return fake_task_batch
}



/*
func Test_UnmarshallPayload(t *testing.T) {
	var fake_task_batch = consumer.Batch{1,{0,1,2,3}}
	var fake_task_payload = producer.TaskPayload{1,{0,1,2,3}}
	fake_taskBytes, _ := json.Marshal(fake_task_payload)
	result := unmarshallPayload(fake_taskBytes)
	if fake_task_batch.Id != result.Id{
		t.Error("Id in payload doesn't match that of the payload passed in")
	}
	if fake_task_batch.Nums != result.Nums{
		t.Error("Nums array in payload doesn't match that of the payload passed in")
	}
}
*/

func appendPayload_stub(task Batch) [][]int {
	fake_allNums := make([][]int, 10)
	for i := 0; i < 10; i++ {
		fake_allNums[i] = make([]int, 100000)
	}

	return fake_allNums
}

/*
func Test_appendPayload(t *testing.T) {
	var temp_arr [100000]int
	var fake_task_batch = consumer.Batch{1,temp_arr}
	var allNums [9][100000]int
	append_payload(fake_taskBytes)
	if len(allNums) != 10{
		t.Error("after apend temp_arr to allNums, size of allNums should be 10")
	}
	if allNums[9] != temp_arr{
		t.Error("last row of temp_arr is not the array passed in")
	}
}
*/

func mergeTenBatches_stub() []int {
	// var fake_res [1000000]int
	fake_res := make([]int, 1000000)
	return fake_res
}

/*
func Test_mergeTenBatches(t *testing.T) {
	var allNums [10][100000]int
	var expect_res [1000000]int
	after_merge := mergeTenBatches()
	if after_merge != expect_res{
		t.Error("after merge ten empty batches, the result should be an aray of size 10*10w")
	}
	
}
*/

func Test_Consume(t *testing.T) {
    Convey("TestApplyFunc_Consume", t, func() {
		
        patches := ApplyFunc(unmarshallPayload, unmarshallPayload_stub)
		defer patches.Reset()

		patches.ApplyFunc(append_payload, appendPayload_stub)
		
		patches.ApplyFunc(mergeTenBatches, mergeTenBatches_stub)
		
		fake_payload_arr := []int{0,1,2,3}
		fake_task_payload := TaskPayload{1,fake_payload_arr}
		fake_taskBytes, _ := json.Marshal(fake_task_payload)
		var fake_merge_result [1000000]int

		result := Consume(fake_taskBytes)
        So(result, ShouldEqual, fake_merge_result)
        
    })
}


