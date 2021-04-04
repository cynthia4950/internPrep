package test

import (
	"encoding/json"
	"testing"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/agiledragon/gomonkey/v2/test/fake"
	. "github.com/smartystreets/goconvey/convey"

	// "testing"
	// "github.com/golang/mock/gomock"
	// "multiwayMS/mock"
)

var (
    outputExpect = "xxx-vethName100-yyy"
)


func Test_Initializer(t *testing.T) {
    Convey("TestApplyFunc_Initializer", t, func() {
        Convey("one func for succ", func() {
			fake_complete := [10]bool
			for i := 0; i < 10; i++ {
				fake_complete[i] = false
			}
            output1,output2 := initializer()
            So(output1, ShouldEqual, fake_complete) 
            So(output2, ShouldEqual, false)
        })

        
    })
}

func Test_Consume(t *testing.T) {
    Convey("TestApplyFunc_Consume", t, func() {
		var fake_delivery rmq.Delivery
		var fake_task Batch
		var fake_allNums [10][100000]int
		var fake_merge_result [1000000]int
        patches := ApplyFunc(unmarshallPayload, func(fake_delivery rmq.Delivery) Batch {
			fake_task.Id = 0
			fake_task.Nums = {0,1,2,3}
			return fake_task
		})//对函数unmarshallPayload打桩
		defer patches.Reset()

		patches.ApplyFunc(append_payload, func(task Batch) [][]int {
			return fake_allNums
		})//对函数append_payload打桩
		
		patches.ApplyFunc(mergeTenBatches, func() []int{
			return fake_merge_result
		})//对函数mergeTenBatches打桩
		
		result := Consume(fake_delivery)
        So(result, ShouldEqual, fake_merge_result)
        
    })
}

//test the helper functions of consume
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

func Test_mergeTenBatches(t *testing.T) {
	var allNums [10][100000]int
	var expect_res [1000000]int
	after_merge := mergeTenBatches()
	if after_merge != expect_res{
		t.Error("after merge ten empty batches, the result should be an aray of size 10*10w")
	}
	
}