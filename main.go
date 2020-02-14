package main

import (
	"flag"
	"fmt"
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/mqtt"
	"strconv"
	"sync"
	"time"
)

var (
	t    int
	c    int
	h    bool
	addr string
)

func init() {
	flag.BoolVar(&h, "h", false, "Please put baetyl-broker under testdata/bin")
	flag.IntVar(&c, "c", 100000, "Set message counts which should be higher than 10000")
	flag.IntVar(&t, "t", 1, "1: broker exit normally, 2: broker killed by kill 9， 3: connect to a mqtt server for network testing, need addr parameter")
	flag.StringVar(&addr, "addr", "", "set addr in testcase3")
}

func main() {
	flag.Parse()
	if c < 10000 {
		fmt.Println("message count is below 10000")
		flag.Usage()
		return
	}
	switch t {
	case 1:
		test1(c)
	case 2:
		test2(c)
	case 3:
		if addr == "" {
			fmt.Println("addr can't be empty in testcase3")
			flag.Usage()
			return
		}
		test3(c, addr)
	default:
		fmt.Println("no testcase for you")
		flag.Usage()
		return
	}
	fmt.Println("------------->>> end test <<<---------------")
}

func test1(count int) {
	b, err := NewBroker()
	if err != nil {
		fmt.Println(err)
		return
	}
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	topic := "test"
	pubObs := &PubObs{
		cur: 1,
		max: count,
		wg:  &wg1,
	}
	pubCli, err := NewPub("tcp://127.0.0.1:"+strconv.Itoa(port), "baetyl-pub", false, pubObs)
	if err != nil {
		fmt.Println("Init PubCli failed", err)
		return
	}

	subObs := &SubObs{
		channel: make(chan packet.Generic, 50),
		bitMap:  NewBitMap(count),
		max:     count,
		wg:      &wg2,
	}
	topics := []mqtt.QOSTopic{{
		QOS:   1,
		Topic: topic,
	}}
	subCli, err := NewSub("tcp://127.0.0.1:"+strconv.Itoa(port), "baetyl-sub", false, subObs, topics)
	if err != nil {
		fmt.Println("Init SubCli failed!", err)
		return
	}

	wg1.Add(1)
	wg2.Add(1)
	subObs.Start(subCli)
	// sleep for subscribe done
	time.Sleep(time.Second)
	pubCli.Publish(count, topic)
	wg1.Wait()
	fmt.Println("------------->>> finished send all messages <<<---------------")
	b.Stop()
	b, err = NewBroker()
	defer b.Stop()
	if err != nil {
		fmt.Println(err)
		return
	}
	wg2.Wait()
	fmt.Println("------------->>> start checking all messages <<<---------------")
	time.Sleep(5 * time.Second)
	res := subObs.Check()
	if !res {
		panic("check failed")
	}
}

func test2(count int) {
	b, err := NewBroker()
	if err != nil {
		fmt.Println(err)
		return
	}
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	topic := "test"
	pubObs := &PubObs{
		cur: 1,
		max: count,
		wg:  &wg1,
	}
	pubCli, err := NewPub("tcp://127.0.0.1:"+strconv.Itoa(port), "baetyl-pub", false, pubObs)
	if err != nil {
		fmt.Println("Init PubCli failed", err)
		return
	}

	subObs := &SubObs{
		channel: make(chan packet.Generic, 50),
		bitMap:  NewBitMap(count),
		max:     count,
		wg:      &wg2,
	}
	topics := []mqtt.QOSTopic{{
		QOS:   1,
		Topic: topic,
	}}
	subCli, err := NewSub("tcp://127.0.0.1:"+strconv.Itoa(port), "baetyl-sub", true, subObs, topics)
	if err != nil {
		fmt.Println("Init SubCli failed!", err)
		return
	}

	wg1.Add(1)
	wg2.Add(1)
	subObs.Start(subCli)
	// sleep for subscribe done
	time.Sleep(time.Second)
	pubCli.Publish(count, topic)
	wg1.Wait()
	fmt.Println("------------->>> finished send all messages <<<---------------")
	b.Kill()
	b, err = NewBroker()
	defer b.Stop()
	if err != nil {
		fmt.Println(err)
		return
	}
	wg2.Wait()
	fmt.Println("------------->>> start checking all messages <<<---------------")
	time.Sleep(5 * time.Second)
	res := subObs.Check()
	if !res {
		panic("check failed")
	}
}

func test3(count int, addr string) {
	addr = "tcp://" + addr
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	topic := "test"
	pubObs := &PubObs{
		cur: 1,
		max: count,
		wg:  &wg1,
	}
	pubCli, err := NewPub(addr, "baetyl-pub", true, pubObs)
	if err != nil {
		fmt.Println("Init PubCli failed", err)
		return
	}

	subObs := &SubObs{
		channel: make(chan packet.Generic, 50),
		bitMap:  NewBitMap(1000000),
		max:     count,
		wg:      &wg2,
	}
	topics := []mqtt.QOSTopic{{
		QOS:   1,
		Topic: topic,
	}}
	subCli, err := NewSub(addr, "baetyl-sub", false, subObs, topics)
	if err != nil {
		fmt.Println("Init SubCli failed!", err)
		return
	}

	wg1.Add(1)
	wg2.Add(1)
	subObs.Start(subCli)
	// sleep for subscribe done
	time.Sleep(2 * time.Second)
	pubCli.Publish(count, topic)
	wg1.Wait()
	fmt.Println("------------->>> finished send all messages <<<---------------")
	wg2.Wait()
	fmt.Println("------------->>> start checking all messages <<<---------------")
	time.Sleep(5 * time.Second)
	res := subObs.Check()
	if !res {
		panic("check failed")
	}
}
