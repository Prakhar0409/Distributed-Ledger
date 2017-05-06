package node

import "fmt"

type Node struct {
	nodeid int
	messageQ chan Message
	txn [1000]Transaction		//the ledger with a node
}

func Run(id int,quit_ch chan int) {
	fmt.Println("Hello from node:",id)

/*	for i := range msgChan {
		fmt.Println(i)
	}
*/


	//send a exiting signal
	quit_ch <- 1
}
