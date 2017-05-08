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
	ledger_entrynum int
	quit chan int
	Live int
	no_die int 							//this guy is the co-ordinator and cannot die
	txn_num int
	pending_txns map[int]*Transaction		//pending to be committed
	pending_broadcast map[int]bool
	pending_logs map[int]*Log
	pending_commits map[int]*Log 			// txnid -> Log
	commit_logs map[int]*Log 			// txnid -> Log
	max_accepted int
	//set of txns pending to be broadcast
	// simulator Simulator
}

func (n *Node) Initialize(nodeid int, list []Node, maxTxns int, quit chan int){
	n.nodeid = nodeid
	n.node_list = list
	n.messageQ = make(chan Message, 10000)
	n.txn_list = make([]Transaction, maxTxns)
	n.ledger_entrynum = 0
	n.quit = quit
	n.Live = 1
	n.txn_num = 0
	n.pending_txns = make(map[int]*Transaction)
	n.pending_broadcast = make(map[int]bool)
	n.no_die = 0
	n.pending_logs = make(map[int]*Log)
	n.pending_commits = make(map[int]*Log)
	n.commit_logs = make(map[int]*Log)
}

func (n *Node) Run() {
	fmt.Println("[HELLO] from node:",n.nodeid)


	//infinite loop
	for{		

		//die with random probability say 2 in 10,0000
		die := rand.Intn(1000000)
		if die < 1 && n.no_die == 0 {
			fmt.Println("DIE: from node:",n.nodeid)
			n.Live = 0
			break
		}

		//do a transaction with random probability say 1 in 10000
		transact := rand.Intn(10000)
		if transact < 1 {
			n.no_die++
			n.doTransaction()
		}

		//check if for any pending transaction - you wait for more than 1,00,00,000 iterations. send a global abort
		for tid, txn := range n.pending_txns {
			txn.waiting_time++
			if txn.waiting_time > 10000000{
				//Timeout and send a global abort.
				delete(n.pending_txns,tid)
				txn.dest.messageQ <- Message{msgtype: "global_abort", src: n, dest:txn.dest, txn:txn}
				txn.mod.messageQ <- Message{msgtype: "global_abort", src: n, dest:txn.mod, txn:txn}
			}
		}


		select {
			case msg := <- n.messageQ:
				
				if msg.msgtype == "vote_request" {
			        fmt.Printf("[VOTE_REQUEST recv] nodeid: %d txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)

			        commit := rand.Intn(5) > 1 		//commit 4 out of every 5 times
			        if commit {
			            msg.src.messageQ <- Message{msgtype: "vote_commit", src: n, dest: msg.src, txn: msg.txn}
			        } else {
			            //abort the txn
			            msg.src.messageQ <- Message{msgtype: "vote_abort", src: n, dest: msg.src, txn: msg.txn}
			        }
		        } else if msg.msgtype == "vote_commit" {
			        fmt.Printf("[VOTE_COMMIT recv] nodeid: %d txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)

		            msg.txn.num_replies++
		            _,found := n.pending_txns[msg.txn.txnid]	
		            if msg.txn.num_replies >= 2 && found{		//not found -> recevied this message after a timeout & have already sent a global abort
						txn := msg.txn 							//copying a pointer
			            txn.num_replies = 0
						delete(n.pending_txns,txn.txnid) 		//remove from my pending txns
			            
			            msgtype := "global_abort" 
		            	if msg.txn.aborted == false{
							msgtype = "global_commit"
							n.pending_broadcast[txn.txnid] = true	//pending to broadcast
						}	
						txn.dest.messageQ <- Message{msgtype: msgtype, src: n, dest:txn.dest, txn:txn}
						txn.mod.messageQ <- Message{msgtype: msgtype, src: n, dest:txn.mod, txn:txn}
						n.no_die--
		            }

		        } else if msg.msgtype == "vote_abort" {
			        fmt.Printf("[VOTE_ABORT recv] nodeid: %d txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
		            
		            msg.txn.aborted = true;
		            msg.txn.num_replies++
					_,found := n.pending_txns[msg.txn.txnid]
		            if msg.txn.num_replies >= 2 && found{		//if not found -> recevied this message after a timeout & have already sent a global abort
						txn := msg.txn
			            delete(n.pending_txns,txn.txnid) 			//remove from my pending pending_txns
						txn.dest.messageQ <- Message{msgtype: "global_abort", src: n, dest:txn.dest, txn:txn}
						txn.mod.messageQ <- Message{msgtype: "global_abort", src: n, dest:txn.mod, txn:txn}
						n.no_die--
		            }

		        } else if msg.msgtype == "global_abort" {
		        	fmt.Printf("[GLOBAL_ABORT recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
		        } else if msg.msgtype == "global_commit" {
		        	fmt.Printf("[GLOBAL_COMMIT recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
		            
		          // msgs for oredered broadcast
		        }else if msg.msgtype == "request_event_log"{
					accept1:=true
					
					fmt.Printf("[Request_for_logging_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					for i, v := range n.pending_broadcast {
						if(v){							//if pending for broadcast
							if(i < msg.txn.txnid ){
								accept1 = false
								break
							}else if(i == msg.txn.txnid && n.nodeid < msg.src.nodeid ){
								accept1 = false
								break
							}
						}else{
							//should ideally never reach here
							delete(n.pending_broadcast,i)
						}
					}
					if(accept1){
						msg.src.messageQ <- Message{msgtype: "ack_event_log", src: n, dest: msg.src, txn:msg.txn}
					}else{
						n.messageQ <- msg
					}
				}else if msg.msgtype == "ack_event_log"{
					fmt.Printf("[Ack_for_logging_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					
					// Check if recieved from all live nodes
					n.pending_logs[msg.txn.txnid].nodes_recieved[msg.src.nodeid] = true
					committed := true
					for _,v :=  range n.node_list{
						if(v.Live == 1){
							_,found := n.pending_logs[msg.txn.txnid].nodes_recieved[v.nodeid]
							if(!found){
								committed = false
								break;
							}
						}
					}
					
					if (committed){
						msg_send := Message{msgtype: "request_commit_log", src: n, dest: msg.src, txn:msg.txn}
						delete(n.pending_logs, msg.txn.txnid)
						// send to all to commit and add to their ledger
						for _,v :=  range n.node_list{
								if(v.Live == 1){
									v.messageQ <- msg_send
								}
						}
						n.txn_list[n.ledger_entrynum] = *(msg.txn)
						n.ledger_entrynum++
						delete(n.pending_logs,msg.txn.txnid)
						delete(n.pending_broadcast,msg.txn.txnid)
					}
				}else if msg.msgtype == "request_commit_log"{
					fmt.Printf("[Request_for_commiting_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					//Commit the log
					n.txn_list[n.ledger_entrynum] = *(msg.txn)
					n.ledger_entrynum++;
					// Send ack to the main node
					msg.src.messageQ <- Message{msgtype: "ack_commit_log", src: n, dest: msg.src, txn:msg.txn}
					
				}else if msg.msgtype == "ack_commit_log"{
					fmt.Printf("[Ack_for_commiting_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					// Check if recieved by all
					n.commit_logs[msg.txn.txnid].nodes_recieved[msg.src.nodeid] = true
					committed := true
					for _,v :=  range n.node_list{
						if(v.Live==1){
							_,found := n.pending_logs[msg.txn.txnid].nodes_recieved[v.nodeid]
							if(!found){
								committed = false
								break;
							}
						}
					}
					if(committed){
						fmt.Printf("[ALL_ACKS_FOR_COMMITING] nodeid: %d  txnid: %d \n", n.nodeid, msg.txn.txnid)
						delete(n.commit_logs,msg.txn.txnid)
					}
					
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
		n.no_die--
		return					/* could not find destination - txn failed */
	}

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
		n.no_die--
		return 				/* could not find moderator - txn failed */
	}

	//Decide amount
	amt := rand.Intn(10)		//assume all have infinite money


	//TODO - generate a txn with txnid > max accepted 
	txn := Transaction{src: n, dest: withNode, mod: modNode, amt:amt, txnid: (n.nodeid + n.max_accepted), 
						num_replies:0, aborted:false, waiting_time: 0}
	n.pending_txns[txn.txnid] = &txn

	fmt.Println("[TXN START] txnid",txn.txnid,"  coordinator:",n.nodeid, "  destination: ",withNode.nodeid,"  moderator: ",modNode.nodeid)

	withNode.messageQ <- Message{msgtype: "vote_request", src: n, dest:withNode, txn: &txn}
	modNode.messageQ <- Message{msgtype: "vote_request", src: n, dest:modNode, txn: &txn}
}
