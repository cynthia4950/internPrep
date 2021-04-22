package main

import (
	// "context"
	// "errors"
	"fmt"
	"encoding/json"
	"github.com/gomodule/redigo/redis"
)

type Queue struct {
	queueName string
}

//传输内容进队列：
// With Delivery, producers will send msg into the queue
func (q *Queue) Delivery(msg Message, connection redis.Conn) error {
	// defer connection.Close()
	fmt.Println("call q.Delivery()")
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	} else {
		//实际传输：
		_, err := connection.Do("LPUSH", q.queueName, msgBytes)
		// fmt.Println("produced", string(msgJson))
		if err != nil {
			return err
		}
		fmt.Println("LPUSH success in q.Delivery")
	}
	return nil
}
