package node

import (
	"fmt"
	"math/rand"
)

type Node struct {
	nodeid int
	node_list []Node
	messageQ chan Message
	txn_list []Transaction					//the ledger with a node
	quit chan int
	Live int
	txn_num int
	pending_txns map[int]bool				//pending to be committed
	pending_broadcast map[int]bool 			//set of txns pending to be broadcast
	// simulator Simulator
}

func (n *Node) Initialize(nodeid int, list []Node, maxTxns int, quit chan int){
	n.nodeid = nodeid
	n.node_list = list
	n.messageQ = make(chan Message, 10000)
	n.txn_list = make([]Transaction, maxTxns)
	n.quit = quit
	n.Live = 1
	n.txn_num = 0
	pending_txns = make(map[int]bool)
}

func (n *Node) Run() {
	fmt.Println("[HELLO] from node:",n.nodeid)


	//infinite loop
	for{		

		//die with random probability say 2 in 10,000
		die := rand.Intn(10000)
		if die <= 1 {
			fmt.Println("DIE: from node:",n.nodeid)
			n.Live = 0
			break
		}

		//do a transaction with random probability say 1 in 100
		transact := rand.Intn(100)
		if transact < 1 {
			n.doTransaction()
		}

		msg := n.messageQ
		if msg.msgtype == "vote_request" {
	        fmt.Println("VOTE_REQUEST: nodeid: %d receives txnid: %d", n.nodeid,msg.txn.txnid)
	        
	        commit := rand.Intn(5) > 1 		//commit 4 out of every 5 times
	        if commit {
	            n.pending_txns[msg.txn.txnid] = true
	            msg.src.messageQ <- Message{msgtype: "vote_commit", src:n, dest: msg.src, txn:msg.txn}
	        } else {
	            //abort the txn
	            msg.src.messageQ <- Message{msgtype: "vote_abort", src:n, dest: msg.src, txn:msg.txn}
	        }
        } else if msg.msgtype == "vote_commit" {
            msg.txn.num_replies++
            if msg.txn.num_replies >= 2{
				txn := msg.txn
	            txn.num_replies = 0
	            delete(n.pending_txns,txn.txnid) //remove from my pending txns
	            n.pending_broadcast[txn.txnid] = true	//pending to broadcast
	            msgtype := "global_abort" 
            	if msg.txn.aborted == false{
            		//do a global commit
					msgtype := "global_commit" 
				}	
				txn.dest.messageQ <- Message{msgtype: msgtype, src:n, dest:txn.dest, txn:txn}
				txn.mod.messageQ <- Message{msgtype: msgtype, src:n, dest:txn.mod, txn:txn}
            }
        } else if msg.msgtype == "vote_abort" {
            msg.txn.num_replies++
            msg.txn.aborted = true;
            if msg.txn.num_replies >= 2{
            	//chill bro - do nothing
            }else{
				txn := msg.txn
	            txn.num_replies = 0
	            delete(n.pending_txns,txn.txnid) //remove from my pending txns
            }
        } else if msg.msgtype == "global_abort" {
            _, found = n.pending_txns[msg.txn.txnid]
            if found{
				delete(n.pending_txns,txn.txnid) //remove from my pending txns
            }
        } else if msg.msgtype == "global_commit" {
            _, found = n.pending_txns[msg.txn.txnid]
            if found{
				delete(n.pending_txns,txn.txnid) //remove from my pending txns
            }
            //do nothing more. wait for the main guy to broadcast
        }

	}


	//send a exiting signal
	n.quit <- 1
}

func (n *Node) doTransaction(){
	n.txn_num++
	//dest node
	withNode := nil
	found := false
	while(!found){
		withNode := rand.Intn(len(n.node_list))
		if n.node_list[withNode].Live == 1{
			found = true
		}
	}

	//Decide amount
	amt := rand.Intn(10)		//assume all have infinite money

	//choose moderator
	modNode := nil
	found = false
	while(!found){
		modNode := rand.Intn(len(n.node_list))
		if n.node_list[modNode].Live == 1{
			found = true
		}
	}

	fmt.Println("[TXN START] by node:",n.nodeid, "  with node: ",withNode," and third party:",modNode)

	/* do a 2-phase commit. I am co-ordinator. withNode is dest and modNode is moderator
	//IMPORTANT to prevent deadlock. channel send is also blocking. - done. made channel size verylarge
	1. generate txn, 2. send
	*/

	txn := Transaction{src: n, dest: withNode, mod: modNode, amt:amt, txnid: (n.nodeid*10000 + n.txn_num), num_replies:0, aborted:false}
	n.pending_txns[txn.txnid] = true
	withNode.messageQ <- Message{msgtype: "vote_request", src:n, dest:withNode, txn:txn}
	modNode.messageQ <- Message{msgtype: "vote_request", src:n, dest:modNode, txn:txn}

	//vote_request done
}
