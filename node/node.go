package node

import (
	"fmt"
	"math/rand"
	"reflect"
)

type Node struct {
	nodeid int
	node_list []Node
	messageQ chan Message
	txn_list []Transaction					//the ledger with a node
	quit chan int
	Live int
	no_die bool 							//this guy is the co-ordinator and cannot die
	txn_num int
	pending_txns map[int]*Transaction		//pending to be committed
	pending_broadcast map[int]bool
	max_accepted int
	//set of txns pending to be broadcast
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
	n.pending_broadcast = make(map[int]bool)
	n.no_die = false
}

func (n *Node) Run() {
	fmt.Println("[HELLO] from node:",n.nodeid)


	//infinite loop
	for{		

		//die with random probability say 2 in 10,0000
		die := rand.Intn(1000000)
		if die < 1 && n.no_die == false {
			fmt.Println("DIE: from node:",n.nodeid)
			n.Live = 0
			break
		}

		//do a transaction with random probability say 1 in 10000
		transact := rand.Intn(10000)
		if transact < 1 {
			n.no_die = true
			n.doTransaction()
		}

		//check if for any pending transaction - you wait for more than 10,00,000 iterations. send a global abort
		for tid, txn := range n.pending_txns {
			if !txn.aborted {
				delete(n.pending_txns,tid)
			}else{
				txn.waiting_time++
				if txn.waiting_time > 1000000{
					//Timeout and send a global abort.
				}
			}

		}


		select {
			case msg := <- n.messageQ:
				
				if msg.msgtype == "vote_request" {
			        fmt.Printf("[VOTE_REQUEST recv] nodeid: %d txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
			        
			        commit := rand.Intn(5) > 1 		//commit 4 out of every 5 times
			        if commit {
			            msg.src.messageQ <- Message{msgtype: "vote_commit", src: n, dest: msg.src, txn:msg.txn}
			        } else {
			            //abort the txn
			            msg.src.messageQ <- Message{msgtype: "vote_abort", src: n, dest: msg.src, txn:msg.txn}
			        }
		        } else if msg.msgtype == "vote_commit" {
			        fmt.Printf("[VOTE_COMMIT recv] nodeid: %d txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
		            msg.txn.num_replies++
		            if msg.txn.num_replies >= 2{
						txn := msg.txn 						//copying a pointer
						fmt.Println("[TYPE POINTER OR TXN]", reflect.TypeOf(txn))

			            txn.num_replies = 0

			            delete(n.pending_txns,txn.txnid) 	//remove from my pending txns
			            msgtype := "global_abort" 
		            	if msg.txn.aborted == false{
		            		//do a global commit
							msgtype = "global_commit"
							n.pending_broadcast[txn.txnid] = true	//pending to broadcast
						}	
						txn.dest.messageQ <- Message{msgtype: msgtype, src: n, dest:txn.dest, txn:txn}
						txn.mod.messageQ <- Message{msgtype: msgtype, src: n, dest:txn.mod, txn:txn}
						n.no_die = false
		            }
		        } else if msg.msgtype == "vote_abort" {
			        fmt.Printf("[VOTE_ABORT recv] nodeid: %d txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
		            msg.txn.aborted = true;
		            msg.txn.num_replies++
		            if msg.txn.num_replies >= 2{
		            	n.no_die = false
						txn := msg.txn
			            delete(n.pending_txns,txn.txnid) //remove from my pending txns
						txn.dest.messageQ <- Message{msgtype: "global_abort", src: n, dest:txn.dest, txn:txn}
						txn.mod.messageQ <- Message{msgtype: msgtype, src: n, dest:txn.mod, txn:txn}
		            }
		        } else if msg.msgtype == "global_abort" {
		        	fmt.Printf("[GLOBAL_ABORT recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
		            _, found := n.pending_txns[msg.txn.txnid]
		            if found{
						delete(n.pending_txns,msg.txn.txnid) //remove from my pending txns
		            }
		        } else if msg.msgtype == "global_commit" {
		        	fmt.Printf("[GLOBAL_COMMIT recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
		            _, found := n.pending_txns[msg.txn.txnid]
		            if found{
						delete(n.pending_txns,msg.txn.txnid) //remove from my pending txns
		            }
		            //do nothing more. wait for the main guy to broadcast
		        }else if msg.msgtype == "request_event_log"{
					accept1:=true
					
					fmt.Printf("[Request_for_logging_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					for i, v := range n.pending_broadcast {
						if(v){
						if(i < msg.txn.txnid ){
							accept1 = false
							break
						}
						if(i == msg.txn.txnid && n.nodeid < msg.src.nodeid ){
							accept1 = false
							break
						}
						}
					}
					if(accept1){
						msg.src.messageQ <- Message{msgtype: "ack_event_log", src: n, dest: msg.src, txn:msg.txn}
					}else{
						n.messageQ <- msg
					}
				}else if msg.msgtype == "ack_event_log"{
					fmt.Printf("[Ack_for_logging_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					// Check if recieved by all
					
				}else if msg.msgtype == "request_commit_log"{
					fmt.Printf("[Request_for_commiting_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					//Commit the log
					
					
					// Send ack to the main node
					msg.src.messageQ <- Message{msgtype: "ack_commit_log", src: n, dest: msg.src, txn:msg.txn}
					
				}else if msg.msgtype == "ack_commit_log"{
					fmt.Printf("[Ack_for_commiting_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					// Check if recieved by all
					
					// Remove from pending broadcast if recieved by all
					
				}else if msg.msgtype == "view_change"{
					
					// Update the nodelist accordingly
				}else if msg.msgtype =="request_msg_flushed"{
					// Update that msg update from that node
				}else if msg.msgtype =="gossip_share"{
					// Share your matrix with someone else.
				}else if msg.msgtype =="msg_deliverd" {
					
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
	
	//find a dest node
	var withNode *Node
	found1 := false
	for i := 0; i < len(n.node_list); i++ {
		if n.node_list[i].Live == 1 && n.node_list[i].nodeid != n.nodeid {
			found1 = true
			withNode = &n.node_list[i]
			break
		}
	}
	if !found1{
		return					/* could not find destination - txn failed */
	}

	//Decide amount
	amt := rand.Intn(10)		//assume all have infinite money

	//choose moderator node
	var modNode *Node
	found2 := false
	for i := 0; i < len(n.node_list); i++ {
		if n.node_list[i].Live == 1 && n.node_list[i].nodeid != withNode.nodeid && n.node_list[i].nodeid != n.nodeid{
			found2 = true
			modNode = &n.node_list[i]
			break
		}
	}

	if !found2 {
		return 				/* could not find moderator - txn failed */
	}


	/* do a 2-phase commit. I am co-ordinator. withNode is dest and modNode is moderator
	//IMPORTANT to prevent deadlock. channel send is also blocking. - done. made channel size verylarge
	1. generate txn, 2. send
	*/

	txn := Transaction{src: n, dest: withNode, mod: modNode, amt:amt, txnid: (n.nodeid*10000 + n.txn_num), num_replies:0, aborted:false}
	n.pending_txns[txn.txnid] = &txn

	fmt.Println("[TXN START] txnid",txn.txnid,"  coordinator:",n.nodeid, "  destination: ",withNode.nodeid,"  moderator: ",modNode.nodeid)

	withNode.messageQ <- Message{msgtype: "vote_request", src: n, dest:withNode, txn: &txn}
	modNode.messageQ <- Message{msgtype: "vote_request", src: n, dest:modNode, txn: &txn}

	//vote_request done
}
