package main

import (
	"testing"
	// "reflect"
	// . "github.com/agiledragon/gomonkey"
	. "github.com/smartystreets/goconvey/convey"
)


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

/*
func Test_SendPayload(t *testing.T) {
	//involve connection
}
*/



/*
func openConnAndQueue_stub() rmq.Queue{
	//involve connection
}
*/
