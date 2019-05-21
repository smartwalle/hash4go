package main

import (
	_ "crypto/sha256"
	"fmt"
	"github.com/smartwalle/hash4go"
)

func main() {
	var m = hash4go.NewConsistentHash(nil)
	m.Add("a", 1)
	m.Add("b", 1)
	m.Add("c", 1)
	m.Add("d", 1)
	m.Add("e", 1)

	fmt.Println(m.Get("a"))
	fmt.Println(m.Get("b"))
	fmt.Println(m.Get("c"))
	fmt.Println(m.Get("d"))
}
