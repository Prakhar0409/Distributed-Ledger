package main

import (
	"fmt"
//	"/node"
	"github.com/prakhar0409/Distributed-Ledger/node"
)

func main(){
	num_nodes := 10
	var node_list [1000]node.Node
	_ = node_list[1]
	fmt.Printf("node list inited\n")
	quit := make(chan int)
	fmt.Printf("Simulator Started\n")

	for i:=0;i<num_nodes;i++{
		go node.Run(i,quit)
	}

	num_quits := 0
	ok := false
	for{
		_,ok = <-quit
		num_quits++
		if ok && num_quits >= num_nodes{
			break
		}
	}
	fmt.Printf("Ending Simulations\n")
}
