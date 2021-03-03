package main

import (
	"github.com/nats-io/stan.go"
	"log"
	"strconv"
	"sync"
	"time"
)

func main() {

	clusterID := "test-cluster"
	clientIDpub := "test-client-pub"
	clientIDsub := "test-client-sub"
	channel   := "foo"
	durableID := "test-durable"

	for i := 1; i < 3; i++ {
		go sub(clusterID, clientIDsub + strconv.Itoa(i), channel, durableID)
	}

	go publishMessages(clusterID, clientIDpub, channel)

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}


func publishMessages(clusterID string, clientID string, channel string) {
	sc, err := stan.Connect(
		clusterID,
		clientID,
		stan.NatsURL(stan.DefaultNatsURL),
	)
	if err != nil {
		log.Print(err)
		return
	}
	defer sc.Close()

	for i := 0; i < 1001; i++ {
		message := "message " + strconv.Itoa(i)
		sc.Publish(channel, []byte(message))
		time.Sleep(time.Microsecond * 4)
	}
}

func sub(clusterID string, clientID string, channel string, durableID string) {

	sc, err := stan.Connect(
		clusterID,
		clientID,
		stan.NatsURL(stan.DefaultNatsURL),
	)

	if err != nil {
		log.Fatal(err)
	}

     var c int
	// Subscribe with manual ack mode, and set AckWait to 60 seconds
	aw, _ := time.ParseDuration("60s")
	sc.QueueSubscribe(channel,"test", func(msg *stan.Msg) {
		time.Sleep(time.Microsecond)
		msg.Ack() // Manual ACK
		if !msg.Redelivered{
			defer func() {c++
				log.Println(clientID,c)
			}()
		}else{
			log.Println(msg.Redelivered)
		}


	}, stan.DurableName(durableID),
		stan.MaxInflight(1),
		stan.SetManualAckMode(),
		stan.AckWait(aw),
	)

}