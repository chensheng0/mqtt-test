package main

import (
	"fmt"
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/mqtt"
	"strconv"
	"sync"
	"time"
)

const maxID = uint16(65535)

type Publisher struct {
	cli *mqtt.Client
}

type PubObs struct {
	cur   uint16
	max   int
	count int
	wg    *sync.WaitGroup
}

// New new a mqtt client for publishing
func NewPub(addr string, cid string, cleansession bool, obs mqtt.Observer) (*Publisher, error) {
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
	pub := &Publisher{
		cli: cli,
	}
	return pub, nil
}

// Publish publish message and return when all checked
func (p *Publisher) Publish(max int, topic string) {
	for i := 1; i <= max; i++ {
		err := p.cli.Publish(1, topic, []byte(strconv.Itoa(i)), 0, false, false)
		if err != nil {
			panic(err)
		}
	}
}

func (obs *PubObs) OnPublish(packet *packet.Publish) error {
	fmt.Println("Subscriber OnPublisher")
	return nil
}

func (obs *PubObs) OnPuback(packet *packet.Puback) error {
	if obs.cur != uint16(packet.ID) {
		panic("Publisher packet missing")
	}
	if obs.cur == maxID {
		obs.cur = 1
	} else {
		obs.cur++
	}
	obs.count++
	if obs.count == obs.max {
		obs.wg.Done()
	}
	return nil
}

func (obs *PubObs) OnError(err error) {
	fmt.Println("Publisher onError:", err)
}
