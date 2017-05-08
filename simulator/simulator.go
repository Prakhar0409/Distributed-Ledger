package main

import (
	"fmt"
	"os"
	"strconv"
	"github.com/prakhar0409/Distributed-Ledger/node"
	"reflect"
)



func main(){
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run simulator.go <num_nodes>")
		return
	}
	
	maxTxns := 1000
	num_nodes, err := strconv.Atoi(os.Args[1])
    if err != nil  {
        fmt.Println("Panic: Incorrect arguments")
        return
    }

    node_list := make([]node.Node, num_nodes)					//pointer to array
    fmt.Println(reflect.TypeOf(node_list))

   	quit := make(chan int)										//pointer to channel
	for i := 0; i < num_nodes; i++ {
		node_list[i].Initialize(i,node_list,maxTxns,quit);		//array, channel, maps are pointers
		// fmt.Println(node_list[i].nodeid);	//cant refer to unexported field or method
	}

	for i := 0; i < num_nodes; i++ {
		go node_list[i].Run();
	}


	//channel for quitting
	num_quits := 0
	ok := false
	for{
		_,ok = <-quit
		num_quits++
		if ok && num_quits >= num_nodes{
			break
		}
	}
    // <- quit
}
