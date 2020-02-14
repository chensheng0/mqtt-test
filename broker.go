package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
)

var port int

func init() {
	os.RemoveAll("testdata/var")
	os.RemoveAll("testdata/etc")
	hostName := "127.0.0.1"
	var err error
	port, err = GetAvailablePort(hostName)
	if err != nil {
		panic(err)
	}
	t := `
addresses:
  - tcp://0.0.0.0:port
principals:
  - username: test
    password: hahaha
    permissions:
      - action: 'pub'
        permit: ['#']
      - action: 'sub'
        permit: ['#']
logger:
  path: var/log/baetyl/service.log
  level: info
`
	conf := []byte(strings.Replace(t, "port", strconv.Itoa(port), -1))
	err = os.MkdirAll("testdata/etc/baetyl", 0755)
	if err != nil {
		panic(fmt.Errorf("failed to make directory: testdata/etc/baetyl"))
	}
	err = ioutil.WriteFile("testdata/etc/baetyl/service.yml", conf, 0644)
	if err != nil {
		panic(err)
	}
}

type Broker struct {
	p *os.Process
}

func NewBroker() (*Broker, error) {
	err := os.Chdir("testdata")
	defer os.Chdir("..")
	if err != nil {
		return nil, err
	}
	var procAttr os.ProcAttr
	procAttr.Files = []*os.File{nil, os.Stdout, os.Stderr}
	p, err := os.StartProcess("./bin/baetyl-broker", []string{"./bin/baetyl-broker"}, &procAttr)
	if err != nil {
		return nil, err
	}
	b := &Broker{p: p}
	return b, nil
}

func (b *Broker) Stop() {
	b.p.Signal(syscall.SIGTERM)
	b.p.Wait()
	fmt.Println("broker stopped")
}

func (b *Broker) Kill() {
	b.p.Kill()
	fmt.Println("broker was killed")
}

// GetAvailablePort finds an available port
func GetAvailablePort(host string) (int, error) {
	address, err := net.ResolveTCPAddr("tcp", host+":0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
