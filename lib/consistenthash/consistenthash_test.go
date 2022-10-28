package consistenthash

import (
	"fmt"
	"testing"
)

func TestHash(t *testing.T) {
	m := New(3, nil)
	m.AddNode("a", "b", "c", "d")

	fmt.Println(m.PickNode("g"))
	fmt.Println(m.PickNode("h"))
	fmt.Println(m.PickNode("i"))
	fmt.Println(m.PickNode("c"))
	fmt.Println(m.PickNode("z"))
	fmt.Println(m.PickNode("zxc"))

	if m.PickNode("zxc") != "c" {
		t.Error("wrong answer")
	}
}
