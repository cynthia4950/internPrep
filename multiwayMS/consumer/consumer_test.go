package main

import (
	"testing"

	// "reflect"
	// "fmt"
	"os"
	. "github.com/agiledragon/gomonkey"
	. "github.com/smartystreets/goconvey/convey"
	// "github.com/adjust/rmq/v3"
	// "github.com/golang/mock/gomock"
	// "multiwayMS/mock"
	// "strconv"
)

/*
mockgen -source=./consumer/consumer.go -destination=./mock/consumer_mock.go -package=mock
check coverage:
go test -coverprofile=coverage.out
go tool cover -func=coverage.out
go tool cover -html=coverage.out
*/

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

func Test_NewConsumer(t *testing.T) {
	Convey("TestApplyFunc", t, func() {
        Convey("Test new consumer", func() {
			output_real := NewConsumer(17)
			So(output_real.id, ShouldEqual, 17)
		})
	})	
}

func Test_createOutputFile(t *testing.T) {
	Convey("TestApplyFunc", t, func() {
        Convey("Test createOutputFile", func() {
			var fake_fileHandle *os.File
			output_real := createOutputFile(&fake_fileHandle, "../data/output.txt")
			So(output_real, ShouldEqual, true)
		})
	})	
}

/*
func Test_unmarshallPayload(t *testing.T) {
	//cannot test
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
	
	//Question: why slice comparison fail for the following case?
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


func Test_merge(t *testing.T) {
    Convey("TestApplyFunc", t, func() {
        Convey("Test merge", func() {
			fake_complete := make([]bool, 3)
			var fake_allNums [][]int
			temp1 := []int{4,7}
			temp2 := []int{1,5}
			temp3 := []int{8,9}
			fake_allNums = append(fake_allNums,temp1)
			fake_allNums = append(fake_allNums,temp2)
			fake_allNums = append(fake_allNums,temp3)
            fake_indexPtrs := make([]int, 3)
			fake_numArrays := 3
			fake_inputSize  := 2

			output_expect := []int{1,4,5,7,8,9}
			patches := ApplyFunc(createOutputFile, func(_ **os.File, _ string) bool {
                return true
            })
            defer patches.Reset()
			
			/*
			findMin_outputs := []OutputCell{
                {Values: Params{1, 1}},
                {Values: Params{4, 0}},
                {Values: Params{5, 1}},
				{Values: Params{7, 0}},
                {Values: Params{8, 2}},
                {Values: Params{9, 2}},
            }
			patches := ApplyFuncSeq(findMinNum, findMin_outputs)
			defer patches.Reset()

			checkCompleArr_outputs := []OutputCell{
                {Values: Params{false}},
                {Values: Params{false}},
                {Values: Params{false}},
				{Values: Params{false}},
                {Values: Params{false}},
                {Values: Params{true}},
            }
			
			patches = ApplyFuncSeq(checkCompleArr, checkCompleArr_outputs)
			*/

			output_real := merge(fake_complete, fake_allNums, fake_indexPtrs, fake_numArrays, fake_inputSize)

			So(len(output_real), ShouldEqual, len(output_expect))
			for i := 0; i < len(output_real); i++ {
				So(output_real[i], ShouldEqual, output_expect[i])
			}
        })
    })
}



func Test_mergeTenBatches(t *testing.T) {
	Convey("TestApplyFunc", t, func() {
		
        Convey("Test mergeTenBatches", func() {
			output_expect := []int{1,4,5,7,8,9}
			patches := ApplyFunc(merge, func(_ []bool, _ [][]int, _ []int, _ int, _ int) []int {
                return output_expect
            })
            defer patches.Reset()

			var fake_allNums [][]int
			temp1 := []int{4,7}
			temp2 := []int{1,5}
			temp3 := []int{8,9}
			fake_allNums = append(fake_allNums,temp1)
			fake_allNums = append(fake_allNums,temp2)
			fake_allNums = append(fake_allNums,temp3)

			fake_numArrays := 3

			output_real := mergeTenBatches(fake_allNums, fake_numArrays)

			So(len(output_real), ShouldEqual, len(output_expect))
			for i := 0; i < len(output_real); i++ {
				So(output_real[i], ShouldEqual, output_expect[i])
			}
		})
	})
}


/*
func Test_unmarshallPayload(t *testing.T) {
	Convey("TestApplyFunc", t, func() {
        Convey("Test unmarshallPayload", func() {
			test_data := []byte(`
					{
						"Id": 18,
						"Nums": [
							0,
							3,
						]
					}
				`)
			var fake_delivery rmq.Delivery
			fake_delivery.payloads = test_data
			output_real := unmarshallPayload(fake_delivery)
			So(output_real.Id, ShouldEqual, 18)
			So(len(output_real.Nums), ShouldEqual, 2)
			output_expect := []int{0,3}
			for i := 0; i < len(output_real); i++ {
				So(output_real[i], ShouldEqual, output_expect[i])
			}

		})
	})
}
*/

/*
func Test_OpenConnAndProcess(t *testing.T) {
	
}
*/


