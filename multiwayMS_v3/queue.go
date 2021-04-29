package main

import (
	// "context"
	// "errors"
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

type QueueHandler interface {
	Connect() redis.Conn
	Delivery(msg Message, connection redis.Conn) error
	Retrieve(redis.Conn) (interface{}, error)
	// Retrieve(redis.Conn) ([]byte, error)
	GetQueueName() string
}

type Queue struct {
	queueName string
}

type mockQueue struct {
	//mock for general cases and empty redis message queue
	queueName string
}

type mockQueue2 struct {
	//mock for non-empty redis message queue
	queueName string
}

func (q *Queue) GetQueueName() string {
	return q.queueName
}

func (q *mockQueue) GetQueueName() string {
	return q.queueName
}
func (q *mockQueue2) GetQueueName() string {
	return q.queueName
}

func (q *Queue) Connect() redis.Conn {
	redis_conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
	}
	// defer redis_conn.Close()
	return redis_conn
}

func (q *mockQueue) Connect() redis.Conn {
	var dummyConn redis.Conn
	return dummyConn
}
func (q *mockQueue2) Connect() redis.Conn {
	var dummyConn redis.Conn
	return dummyConn
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

func (q *mockQueue) Delivery(msg Message, connection redis.Conn) error {
	// defer connection.Close()
	fmt.Println("call q.Delivery()")
	_, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return nil
}

func (q *mockQueue2) Delivery(msg Message, connection redis.Conn) error {
	// defer connection.Close()
	fmt.Println("call q.Delivery()")
	_, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return nil
}

func (q *Queue) Retrieve(redis_conn redis.Conn) (interface{}, error) {
	item, err := redis_conn.Do("LPOP", rmqName)
	// converted_item, ok := item.(*[]byte)
	/*
		if !ok {
			fmt.Println("!ok: cannot assert reply as type []byte")
		}
	*/
	return item, err
}

func (q *mockQueue) Retrieve(redis_conn redis.Conn) (interface{}, error) {
	// dummy_msg := Message{queueName: "dummy", Content: []int{0,1,2,3,4}}
	// item_fake, err := json.Marshal(dummy_msg)
	// return item_fake,err
	return nil, nil
}

func (q *mockQueue2) Retrieve(redis_conn redis.Conn) (interface{}, error) {
	dummy_msg := Message{queueName: "dummy", Content: []int{0, 1, 2, 3, 4}}
	item_fake, err := json.Marshal(dummy_msg)
	return item_fake, err
	// return nil, nil
}
