package main

import (
	"fmt"
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/mqtt"
	"strconv"
	"sync"
	"time"
)

type SubObs struct {
	channel chan packet.Generic
	bitMap  *BitMap
	wg      *sync.WaitGroup
	max     int
}

func NewSub(addr string, cid string, cleansession bool, obs mqtt.Observer, topics []mqtt.QOSTopic) (*mqtt.Client, error) {
	conf := mqtt.ClientConfig{
		Address:      addr,
		Username:     "test",
		Password:     "hahaha",
		ClientID:     cid,
		CleanSession: cleansession,
		Timeout:      30 * time.Second,
	}
	cli, err := mqtt.NewClient(conf, obs)
	if err != nil {
		return nil, err
	}
	var subs []mqtt.Subscription
	for _, topic := range topics {
		subs = append(subs, mqtt.Subscription{Topic: topic.Topic, QOS: mqtt.QOS(topic.QOS)})
	}
	if len(subs) > 0 {
		err = cli.Subscribe(subs)
		if err != nil {
			return nil, err
		}
	}
	return cli, nil
}

func (s *SubObs) OnPublish(packet *packet.Publish) error {
	if packet.Message.QOS == 1 {
		puback := mqtt.NewPuback()
		puback.ID = packet.ID
		select {
		case s.channel <- puback:
		}
	}
	i, err := strconv.ParseInt(string(packet.Message.Payload), 10, 64)
	if err != nil {
		panic("Subscriber Parse error")
	}
	s.bitMap.Add(int(i))
	if int(i) == s.max {
		s.wg.Done()
	}
	return nil
}

func (s *SubObs) OnPuback(packet *packet.Puback) error {
	fmt.Println("Subscriber OnPuback: ", packet)
	return nil
}

func (s *SubObs) OnError(err error) {
	fmt.Println("Subscriber OnError: ", err)
}

func (s *SubObs) Start(cli *mqtt.Client) {
	go func() {
		for {
			select {
			case pkt := <-s.channel:
				cli.Send(pkt)
			}
		}
	}()
}

func (s *SubObs) Check() bool {
	for i := 1; i < s.max; i++ {
		if !s.bitMap.IsExist(i) {
			fmt.Println("msg is missing: ", i)
			return false
		}
	}
	fmt.Println("check passed")
	return true
}
