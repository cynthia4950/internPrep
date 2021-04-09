package main

import (
	"testing"
	// "reflect"
	// . "github.com/agiledragon/gomonkey"
	// "github.com/adjust/rmq/v3"
	. "github.com/smartystreets/goconvey/convey"
	"encoding/json"
	// "multiwayMS/mock"
	// "github.com/golang/mock/gomock"
)

//mockgen -source=./producer/producer.go -destination=./mock/producer_mock.go -package=mock

func Test_readDataFile(t *testing.T){
	Convey("TestApplyFunc", t, func() {
        Convey("Test createOutputFile", func() {
			test_file_name := "../data/data99.txt"
			output_real := readDataFile(test_file_name)
			output_expect := []int {1,2,3,4,6,7,9}
			So(len(output_real), ShouldEqual, len(output_expect))
			for i := 0; i < len(output_real); i++ {
				So(output_real[i], ShouldEqual, output_expect[i])
			}
		})
	})	
}


func Test_testFilesGenerator(t *testing.T){
	Convey("TestApplyFunc", t, func() {
        Convey("Test testFilesGenerator", func() {
			output_real := testFilesGenerator()
			So(output_real, ShouldEqual, false)
		})
	})	
}

func Test_getContent(t *testing.T){
	Convey("TestApplyFunc", t, func() {
        Convey("Test getContent", func() {
			output_real := getContent(99, "../data/data99.txt")
			data99Content := []int {1,2,3,4,6,7,9}
			fake_task := TaskPayload{99,data99Content}
			output_expect,_ := json.Marshal(fake_task)
			So(len(output_real), ShouldEqual, len(output_expect))
			for i := 0; i < len(output_real); i++ {
				So(output_real[i], ShouldEqual, output_expect[i])
			}
		})
	})	
}



func Test_OpenConnAndQueue_Send(t *testing.T){
	producer_test := RealSend{"localhost:6379"}
	_, err := producer_test.OpenConnAndQueue_Send()
	if err != nil {
		t.Errorf("error occurs in producer's OpenConnAndQueue: %v", err)
	}
}



/*
func Test_main(t *testing.T) {
	//involve connection
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockProducer := mock.NewMockSendingManager(ctl)
	// var fake_queue rmq.Queue
	// fake_bytes := []byte{}
	
	mockProducer.EXPECT().OpenConnAndQueue_Send()
	//.Return(nil)
	// mockProducer.EXPECT().SendPayload(fake_queue, fake_bytes)
	//.Return(nil)
	
	main()
}
*/



