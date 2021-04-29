
package main

import (
	// "encoding/json"
)

type Message struct {
    queueName    string // 投递的目标名称
    Content []int // 要进行序列化的消息内容
}
