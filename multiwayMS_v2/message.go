package main

import (
	"fmt"
	"encoding/json"
	"time"
)

type Message struct {
    name    string // 投递的目标名称
    Content []int `json:"content"` // 要进行序列化的消息内容
}

type MessageProcessor interface {
    Resolve() error
    GetChannel() string
    Marshal() ([]byte, error) 
    Unmarshal([]uint8) (MessageProcessor, error)
}



func (m *Message) GetChannel() string {
    return m.name
}


func (m *Message) Resolve() error {
    // 简单通过打印来表示已经消费。在实际使用中可能是复杂的业务逻辑
	time.Sleep(time.Second)
    // fmt.Printf("consumed %+v\n", m.Content)
    // consume(m)
    fmt.Println("one batch has been consumed")
	//实际消费过程：

    return nil
}


func (m *Message) Marshal() ([]byte, error) {
    return json.Marshal(m)
}

func (m *Message) Unmarshal(reply []byte) (MessageProcessor, error) {
    var msg Message
	err := json.Unmarshal(reply, &msg)
    return &msg, err
}