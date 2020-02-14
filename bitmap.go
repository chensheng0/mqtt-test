package main

import "fmt"

type BitMap struct {
	bits []byte
	max  int
}

//初始化一个BitMap
//一个byte有8位,可代表8个数字,取余后加1为存放最大数所需的容量
func NewBitMap(max int) *BitMap {
	bits := make([]byte, (max>>3)+1)
	return &BitMap{bits: bits, max: max}
}

func (b *BitMap) Add(num int) {
	index := num >> 3
	pos := num & 0x07
	b.bits[index] |= 1 << pos
}

func (b *BitMap) IsExist(num int) bool {
	index := num >> 3
	pos := num & 0x07
	return b.bits[index]&(1<<pos) != 0
}

func (b *BitMap) Remove(num int) {
	index := num >> 3
	pos := num & 0x07
	b.bits[index] = b.bits[index] & ^(1 << pos)
}

func (b *BitMap) Max() int {
	return b.max
}

func (b *BitMap) String() string {
	return fmt.Sprint(b.bits)
}
