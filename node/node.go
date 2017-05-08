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
	pending_txns map[int]*Transaction				//pending to be committed
	pending_broadcast map[int]bool			//set of txns pending to be broadcast
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
	n.pending_txns = make(map[int]*Transaction)
}

func (n *Node) Run() {
	fmt.Println("[HELLO] from node:",n.nodeid)


	//infinite loop
	for{		

		//die with random probability say 2 in 10,000
		// die := rand.Intn(10000)
		// if die <= 1 {
		// 	fmt.Println("DIE: from node:",n.nodeid)
		// 	n.Live = 0
		// 	break
		// }

		//do a transaction with random probability say 1 in 100
		transact := rand.Intn(100)
		if transact < 1 {
			n.doTransaction()

		}


		select {
			case msg := <- n.messageQ:
				
					
				if msg.msgtype == "vote_request" {
			        fmt.Println("VOTE_REQUEST: nodeid: %d receives txnid: %d", n.nodeid,msg.txn.txnid)
			        
			        commit := rand.Intn(5) > 1 		//commit 4 out of every 5 times
			        if commit {
			            msg.src.messageQ <- Message{msgtype: "vote_commit", src: *n, dest: msg.src, txn:msg.txn}
			        } else {
			            //abort the txn
			            msg.src.messageQ <- Message{msgtype: "vote_abort", src: *n, dest: msg.src, txn:msg.txn}
			        }
		        } else if msg.msgtype == "vote_commit" {
					fmt.Println("VOTE_COMMIT: nodeid: %d receives txnid: %d", n.nodeid,msg.txn.txnid)
		            n.pending_txns[msg.txn.txnid].num_replies++
		            if n.pending_txns[msg.txn.txnid].num_replies >= 2{
						txn := msg.txn
			            txn.num_replies = 0
			            delete(n.pending_txns,txn.txnid) //remove from my pending txns
			            msgtype := "global_abort" 
		            	if msg.txn.aborted == false{
		            		//do a global commit
							msgtype = "global_commit"
							n.pending_broadcast[txn.txnid] = true	//pending to broadcast
						}	
						txn.dest.messageQ <- Message{msgtype: msgtype, src: *n, dest:txn.dest, txn:txn}
						txn.mod.messageQ <- Message{msgtype: msgtype, src: *n, dest:txn.mod, txn:txn}
		            }
		        } else if msg.msgtype == "vote_abort" {
		            fmt.Println("VOTE_ABORT: nodeid: %d receives txnid: %d", n.nodeid,msg.txn.txnid)
		            msg.txn.aborted = true;
		            n.pending_txns[msg.txn.txnid].num_replies++
		            if n.pending_txns[msg.txn.txnid].num_replies >= 2{
		            	//chill bro - do nothing
		            }else{
						txn := msg.txn
			            txn.num_replies = 0
			            delete(n.pending_txns,txn.txnid) //remove from my pending txns
		            }
		        } else if msg.msgtype == "global_abort" {
		        	fmt.Println("GLOBAL_ABORT: nodeid: %d receives txnid: %d", n.nodeid,msg.txn.txnid)
		            _, found := n.pending_txns[msg.txn.txnid]
		            if found{
						delete(n.pending_txns,msg.txn.txnid) //remove from my pending txns
		            }
		        } else if msg.msgtype == "global_commit" {
		        	fmt.Println("GLOBAL_COMMIT: nodeid: %d receives txnid: %d", n.nodeid,msg.txn.txnid)
		            _, found := n.pending_txns[msg.txn.txnid]
		            if found{
						delete(n.pending_txns,msg.txn.txnid) //remove from my pending txns
		            }
		            //do nothing more. wait for the main guy to broadcast
		        }

	    	default:
				continue
		}

	}


	//send a exiting signal
	n.quit <- 1
}

func (n *Node) doTransaction(){
	n.txn_num++
	//dest node
	var withNode Node
	found1 := false
	for i := 0; i < len(n.node_list); i++ {
		if n.node_list[i].Live == 1 && n.node_list[i].nodeid != n.nodeid {
			found1 = true
			withNode = n.node_list[i]
			break
		}
	}
	
	//Decide amount
	amt := rand.Intn(10)		//assume all have infinite money

	//choose moderator
	var modNode Node
	found2 := false
	for i := 0; i < len(n.node_list); i++ {
		if n.node_list[i].Live == 1 && n.node_list[i].nodeid != withNode.nodeid && n.node_list[i].nodeid != n.nodeid{
			found2 = true
			modNode = n.node_list[i]
			break
		}
	}



	/* do a 2-phase commit. I am co-ordinator. withNode is dest and modNode is moderator
	//IMPORTANT to prevent deadlock. channel send is also blocking. - done. made channel size verylarge
	1. generate txn, 2. send
	*/

	if !found1 || !found2{
		//failed. cannot transact
		return 
	}

	fmt.Println("[TXN START] by node:",n.nodeid, "  with node: ",withNode.nodeid," and third party:",modNode.nodeid)

	txn := Transaction{src: *n, dest: withNode, mod: modNode, amt:amt, txnid: (n.nodeid*10000 + n.txn_num), num_replies:0, aborted:false, pending: false}
	n.pending_txns[txn.txnid] = &txn
	withNode.messageQ <- Message{msgtype: "vote_request", src: *n, dest:withNode, txn:txn}
	modNode.messageQ <- Message{msgtype: "vote_request", src: *n, dest:modNode, txn:txn}

	//vote_request done
}
