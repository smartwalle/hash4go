package main

import (
	_ "crypto/sha256"
	"fmt"
	"github.com/smartwalle/hash4go"
)

func main() {
	var m = hash4go.NewConsistentHash(nil)
	m.Add("a", 10)
	m.Add("b", 10)
	m.Add("c", 10)
	m.Add("d", 10)
	m.Add("e", 10)

	fmt.Println(m.Get("a"))
	fmt.Println(m.Get("b"))
	fmt.Println(m.Get("c"))
	fmt.Println(m.Get("d"))
}
