package main

import (
	// "encoding/json"
	"testing"
	// "reflect"
	// "fmt"
	// . "github.com/agiledragon/gomonkey"
	// . "github.com/smartystreets/goconvey/convey"
	// "github.com/adjust/rmq/v3"
	// "github.com/golang/mock/gomock"
	// "multiwayMS/mock"
	// "strconv"
)

/*
func Test_OpenConnAndProcess(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	var id int64 = 1
	mockConsuming := mock.NewMockReceivingManager(ctl)
	gomock.InOrder(
		mockConsuming.EXPECT()
	)
}
*/

/*
func unmarshallPayload_stub(delivery rmq.Delivery) Batch {
	temp_arr := []int{0,1,2,3}
	var fake_task_batch = Batch{1,temp_arr}
	return fake_task_batch
}
*/

/*
func Test_marshallANDunmarshall(t *testing.T) {
	var fake_task_batch = Batch{1,{0,1,2,3}}
	var fake_task_payload = TaskPayload{1,{0,1,2,3}}
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

/*
func appendPayload_stub(task Batch) [][]int {
	fake_allNums := make([][]int, 10)
	for i := 0; i < 10; i++ {
		fake_allNums[i] = make([]int, 100000)
	}

	return fake_allNums
}
*/



func Test_appendPayload(t *testing.T) {
	temp_arr := make([]int, 100000)
	for i := 0; i < len(temp_arr); i++ {
		temp_arr[i] = 9
	}
	var fake_task_batch = Batch{1,temp_arr}

	fake_allNums := make([][]int, 9)
	for i := 0; i < 9; i++ {
		fake_allNums[i] = make([]int, 100000)
	}

	append_payload(fake_task_batch, &fake_allNums)
	if len(fake_allNums) != 10{
		t.Error("after apend temp_arr to allNums, size of allNums should be 10")
	}
	// fmt.Println(fake_allNums[len(fake_allNums)-1])
	// fmt.Printf("type of fake_allNums' last row: %T\n", fake_allNums[len(fake_allNums)-1])
	// fmt.Printf("type of temp_arr: %T\n", temp_arr)
	
	for j := 0; j < len(temp_arr); j++{
		if fake_allNums[len(fake_allNums)-1][j] != temp_arr[j] {
			// fmt.Println("differ at index: " + strconv.Itoa(j))
			t.Error("last row of temp_arr is not the array passed in")
		}
	}
	
	//question: why slice comparison fail for the following case?
	/*
	last_row := fake_allNums[len(fake_allNums)-1]
	if reflect.DeepEqual(last_row, temp_arr){
		t.Error("last row of temp_arr is not the array passed in")
	}
	*/
}


func Test_findMinNum(t *testing.T){
	fake_indexPtrs := make([]int, 3) //each row in allNums has a pointer pointing to next number
	fake_complete := make([]bool, 3) //each row in allNums has a boolean representing if all numbers are processed
	var fake_allNums [][]int
	temp1 := []int{4,7}
	temp2 := []int{1,5}
	temp3 := []int{8,9}
	fake_allNums = append(fake_allNums,temp1);
	fake_allNums = append(fake_allNums,temp2);
	fake_allNums = append(fake_allNums,temp3);

	output_tempMin0, output_minArr0 := findMinNum(&fake_complete, &fake_allNums, &fake_indexPtrs, 3)
	if output_tempMin0 != 1 || output_minArr0 != 1 {
		t.Error("0: fail to find min from allNums")
	}
	fake_indexPtrs[output_minArr0]++;

	output_tempMin1, output_minArr1 := findMinNum(&fake_complete, &fake_allNums, &fake_indexPtrs, 3)
	if output_tempMin1 != 4 || output_minArr1 != 0 {
		t.Error("1: fail to find min from allNums")
	}
	fake_indexPtrs[output_minArr1]++;

	output_tempMin2, output_minArr2 := findMinNum(&fake_complete, &fake_allNums, &fake_indexPtrs, 3)
	if output_tempMin2 != 5 || output_minArr2 != 1 {
		t.Error("2: fail to find min from allNums")
	}
	fake_indexPtrs[output_minArr2]++;
	fake_complete[output_minArr2] = true

	output_tempMin3, output_minArr3 := findMinNum(&fake_complete, &fake_allNums, &fake_indexPtrs, 3)
	if output_tempMin3 != 7 || output_minArr3 != 0 {
		t.Error("3: fail to find min from allNums")
	}
	fake_indexPtrs[output_minArr3]++;
	fake_complete[output_minArr3] = true

	output_tempMin4, output_minArr4 := findMinNum(&fake_complete, &fake_allNums, &fake_indexPtrs, 3)
	if output_tempMin4 != 8 || output_minArr4 != 2 {
		t.Error("4: fail to find min from allNums")
	}
	fake_indexPtrs[output_minArr4]++;

	output_tempMin5, output_minArr5 := findMinNum(&fake_complete, &fake_allNums, &fake_indexPtrs, 3)
	if output_tempMin5 != 9 || output_minArr5 != 2 {
		t.Error("5: fail to find min from allNums")
	}
	fake_indexPtrs[output_minArr5]++;
	fake_complete[output_minArr5] = true
}

func Test_checkCompleArr(t *testing.T) {
	fake_indexPtrs := make([]int, 2)
	fake_complete := make([]bool, 2)
	fake_allComplete := false
	checkCompleArr(0, 2, &fake_indexPtrs, &fake_complete, &fake_allComplete)
	if !(fake_complete[0] == false && fake_allComplete == false) {
		t.Error("call 0: fail to update array completion indicators")
	}

	checkCompleArr(1, 2, &fake_indexPtrs, &fake_complete, &fake_allComplete)
	if !(fake_complete[1] == false && fake_allComplete == false) {
		t.Error("call 1: fail to update array completion indicators")
	}

	checkCompleArr(0, 2, &fake_indexPtrs, &fake_complete, &fake_allComplete)
	if !(fake_complete[0] == true && fake_allComplete == false) {
		t.Error("call 2: fail to update array completion indicators")
	}

	checkCompleArr(1, 2, &fake_indexPtrs, &fake_complete, &fake_allComplete)
	if !(fake_complete[1] == true && fake_allComplete == true){
		t.Error("call 3: fail to update array completion indicators")
	}


}

/*
func Test_merge(t *testing.T) {
	output_expect := []int {0,1,2,3,4,5,6,7,8,9}
	fake_allNums := [][]int {{0,1},{4,5},{2,7},{9,3},{6,8}}
	fake_complete := make([]bool, 10) // All false
	fake_indexPtrs := make([]int, 10) // All 0
	output_real := merge(fake_complete, fake_allNums, fake_indexPtrs)
	if reflect.DeepEqual(output_real, output_expect) {
		t.Error("fail to merge values in the 2d array")
	}
}
*/


/*
func mergeTenBatches_stub() []int {
	// var fake_res [1000000]int
	fake_res := make([]int, 1000000)
	return fake_res
}
*/

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

/*
func Test_Consume(t *testing.T) {
	NewConsumer(0)
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
*/

