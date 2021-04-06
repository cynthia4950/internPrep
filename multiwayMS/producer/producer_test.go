package main

import (
	"testing"
	"reflect"
)



/*
func Test_SendPayload(t *testing.T) {
	//involve connection
}
*/

func Test_readDataFile(t *testing.T) {
	output_expect := []int {1,2,3,4,7,9,6}
	//file data99.txt is for testing the "readDataFile" function:
	output_real := readDataFile(99)
	if reflect.DeepEqual(output_real, output_expect) {
		t.Errorf("readDataFile gives = %v, want %v", output_real, output_expect)
	}
}

/*
func openConnAndQueue_stub() rmq.Queue{
	//involve connection
}
*/
