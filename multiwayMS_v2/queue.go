package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
)

type Queue struct {
	pool *redis.Pool
}

//left remove:
func (q *Queue) lrem(queue string, reply interface{}) {
	// redis tip: LREM queueName numbers msg
	conn := q.pool.Get()
	defer conn.Close()
	if _, err := conn.Do("LREM", queue, 1, reply); err != nil {
		fmt.Println("lrem fails", err)
	}
}

func (q *Queue) rpoplpush(imsg MessageProcessor, sourceQueue, destQueue string, block bool) (interface{}, MessageProcessor, error) {
	var r interface{}
	var err error
	conn := q.pool.Get()
	defer conn.Close()

	if block {
		// redis tip: BROPLPUSH sourceQueueName destQueueName timeout
		r, err = conn.Do("BRPOPLPUSH", sourceQueue, destQueue, 1)
	} else {
		// redis tip: ROPLPUSH sourceQueueName destQueueName
		r, err = conn.Do("RPOPLPUSH", sourceQueue, destQueue)
	}

	if err != nil {
		return nil, nil, err
	}
	// return all nil when timeout and read nothing
	if r == nil {
		return nil, nil, nil
	}
	
	rUint8, ok := r.([]uint8)
	if !ok {
		return nil, nil, errors.New("cannot assert reply as type []uint8")
	}
	

	if msg, err := imsg.Unmarshal(rUint8); err != nil {
		return nil, nil, err
	} else if _, ok := msg.(MessageProcessor); ok {
		return r, msg, nil
	} else {
		return nil, nil, errors.New("cannot assert msg as interface IMessage")
	}
}

func (q *Queue) ack(imsg MessageProcessor, sourceQueue, destQueue string) {
	for {
		reply, _, err := q.rpoplpush(imsg, sourceQueue, destQueue, false)
		if err != nil {
			fmt.Println("ack failed", err)
			break
		}
		if reply == nil {
			break
		} else {
			fmt.Printf("got undo msg in the queue %s\n", sourceQueue)
		}
	}
}

// InitReceiver will create goroutine to consume the msg implementing IMessage
// the number decides how much goroutine to do consuming
func (q *Queue) InitReceiver(ctx context.Context, msg MessageProcessor, number int) func() {
	prepareQueue := fmt.Sprintf("%s.prepare", msg.GetChannel())
	xType := fmt.Sprintf("%T", prepareQueue)
	fmt.Println(xType)

	if number <= 0 {
		number = 1
	}

	// the quit channel is used for waiting goroutines exit
	quit := make(chan struct{}, number)
	// the cancel slice store cancel functions for child contexts used in goroutines
	cancelSlice := make([]context.CancelFunc, number)

	for i := 0; i < number; i++ {
		// create child context each per goroutine
		childCtx, cancel := context.WithCancel(ctx)
		// store each cancel function of child contexts into cancel slice
		cancelSlice[i] = cancel

		
		go func(ctx context.Context, number int) {
			doingQueue := fmt.Sprintf("%s.doing%d", msg.GetChannel(), number)
			for {
				select {
				case <-ctx.Done():
					fmt.Println("context has been cancelled")
					quit <- struct{}{}
					return
				default:
				}

				reply, msg, err := q.rpoplpush(msg, prepareQueue, doingQueue, true)
				if err != nil {
					fmt.Println("failed to pop msg", err)
					continue
				}
				if msg == nil {
					continue
				}
				if err := msg.Resolve(); err == nil {
					q.lrem(doingQueue, reply)
				}
				q.ack(msg, doingQueue, prepareQueue)
			}
		}(childCtx, i) // the each variable i must be passed to goroutines immediately
		
		
		// consume(childCtx, i, msg, quit, q)
	}

	cancelFunc := func() {
		for i := 0; i < number; i++ {
			cancel := cancelSlice[i]
			cancel()
			<-quit
		}
	}

	fmt.Printf("%d receivers have been initialized\n", number)
	return cancelFunc
}


//传输内容进队列：
// With Delivery, producers will send msg into the queue
func (q *Queue) Delivery(msg MessageProcessor) error {
	conn := q.pool.Get()
	defer conn.Close()

	prepareQueue := fmt.Sprintf("%s.prepare", msg.GetChannel())
	if msgJson, err := msg.Marshal(); err != nil {
		return err
	} else {
		//实际传输：
		_, err := conn.Do("LPUSH", prepareQueue, msgJson)
		// fmt.Println("produced", string(msgJson))

		return err
	}
}
