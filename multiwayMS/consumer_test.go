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
			/*
            patches := ApplyFunc(initializer, func()([]bool, bool) {
				fake_complete := [10]bool
				for i := 0; i < 10; i++ {
					fake_complete[i] = false
				}
				fake_allComplete := false
                return (fake_complete,fake_allComplete)
            })
            defer patches.Reset()
			*/
            output1,output2 := initializer()
            So(output1, ShouldEqual, fake_complete) 
            So(output2, ShouldEqual, false)
        })

        
    })
}

/*
func Test_Consume(t *testing.T) {
    Convey("TestApplyFunc_Consume", t, func() {
		var fake_task Batch
		var fake_allNums [10][100000]int
		var fake_merge_result [1000000]int
        patches := ApplyFunc(unmarshallPayload, func(delivery rmq.Delivery) Batch {
			task.Id = 0
			task.Nums = {1,2,3}
			return task
		})//对函数unmarshallPayload打桩
		defer patches.Reset()

		patches.ApplyFunc(append_payload, func(task Batch) [][]int {
			return fake_allNums
		})//对函数append_payload打桩
		
		patches.ApplyFunc(mergeTenBatches, func() []int{
			return fake_merge_result
		})//对函数mergeTenBatches打桩
		
		result := Consume(rmq.Delivery)
        So(result, ShouldEqual, 0)
        
    })
}
*/