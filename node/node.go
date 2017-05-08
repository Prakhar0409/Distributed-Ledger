package node

import (
	"fmt"
	"math/rand"
    "strconv"
    "os"
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
	pending_broadcast map[int]*Transaction
	pending_logs map[int]*Log
	pending_commits map[int]*Log 			// txnid -> Log
	commit_logs map[int]*Log 			// txnid -> Log
	max_accepted int
	maxTxns int
	recvd_t [][]int
	recvd_n [][]int
	totalsize int
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
	n.pending_broadcast = make(map[int]*Transaction)
	n.no_die = 0
	n.pending_logs = make(map[int]*Log)
	n.pending_commits = make(map[int]*Log)
	n.commit_logs = make(map[int]*Log)
	n.maxTxns = maxTxns
	nodesize :=len(list)
	n.totalsize = nodesize
	n.recvd_t = make([][]int,nodesize)
	for i:=range n.recvd_t {
		n.recvd_t[i] = make([]int,nodesize)
	}
	n.recvd_n = make([][]int,nodesize)
	for i:=range n.recvd_n {
		n.recvd_n[i] = make([]int,nodesize)
	}
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

		//do a transaction with random probability say 1 in 10000000
		transact := rand.Intn(100000)
		if transact < 1 {
			n.no_die++
			n.doTransaction()
		}
		
		gossip:=rand.Intn(100000)
		if gossip < 1{
			k := rand.Intn(n.totalsize)
			if(n.node_list[k].Live==1 && k!=n.nodeid){
				msg := Message{msgtype: "gossip_share", src: n, dest: &n.node_list[k], rcvd_t : n.recvd_t ,rcvd_n : n.recvd_n}
				n.node_list[k].messageQ <- msg
			}
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

		check := rand.Intn(10000)
		if(check < 1){
			//check if all ack_event_logs received for any of the txns in pending_logs
			for tid,log := range n.pending_logs{ 		// k->txnid, log
				committed := true
				for _,v :=  range n.node_list{
					if(v.Live == 1){
						_,found := n.pending_logs[tid].nodes_recieved[v.nodeid]
						if(!found && v.nodeid!=n.nodeid){
							fmt.Printf("Not recieved from nodeid:%d txnid: %d myself: %d\n",v.nodeid,tid,n.nodeid)
							committed = false
							break;
						}
					}
				}
				
				if (committed){
					// send to all to commit and add to their ledger
					for _,v :=  range n.node_list{
							if(v.Live == 1){
								msg_send := Message{msgtype: "request_commit_log", src: n, dest: &v, txn: log.txn}
								v.messageQ <- msg_send
							}
					}
					n.commit_logs[tid]=  &(Log{txn: log.txn, state: "i dont know", nodes_recieved: make(map[int]bool)})
				}
			}

			//check if all ack_commit_logs received
			for tid,log := range n.pending_logs{ 		// k->txnid, log
				committed := true
				for _,v :=  range n.node_list{
					if(v.Live==1){
						_,found := log.nodes_recieved[v.nodeid]
						if(!found && v.nodeid!=n.nodeid){
							committed = false
							break;
						}
					}
				}
				if(committed){
					fmt.Printf("[ALL_ACKS_FOR_COMMITING] nodeid: %d  txnid: %d \n", n.nodeid, tid)
					//delete from pending_broadcast only if all have replied committed
					delete(n.pending_broadcast,tid)
					delete(n.commit_logs,tid)
					delete(n.pending_logs,tid)
					fmt.Printf("Successful")
				}
				
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
							n.pending_broadcast[txn.txnid] = txn	//pending to broadcast
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
		        } else if msg.msgtype == "request_event_log"{
					accept1:=true
					
					fmt.Printf("[Request_for_logging_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					for i, _ := range n.pending_broadcast {							//if pending for broadcast
						if(i < msg.txn.txnid ){
							accept1 = false
							break
						}else if(i == msg.txn.txnid && n.nodeid < msg.src.nodeid ){
							accept1 = false
							break
						}
					
					}
					if(accept1){
						if(n.max_accepted <= msg.txn.txnid){
							n.max_accepted = msg.txn.txnid
						}
						
						msg.src.messageQ <- Message{msgtype: "ack_event_log", src: n, dest: msg.src, txn:msg.txn}
					}else{
						fmt.Printf("Denying ack from nodeid:%d txnid: %d myself: %d\n",msg.src.nodeid,msg.txn.txnid,n.nodeid)
						n.messageQ <- msg
					}
				} else if msg.msgtype == "ack_event_log"{
					fmt.Printf("[Ack_for_logging_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					
					// Check if recieved from all live nodes
					n.pending_logs[msg.txn.txnid].nodes_recieved[msg.src.nodeid] = true
					committed := true
					for _,v :=  range n.node_list{
						if(v.Live == 1){
							_,found := n.pending_logs[msg.txn.txnid].nodes_recieved[v.nodeid]
							if(!found && v.nodeid!=n.nodeid){
								fmt.Printf("Not recieved from nodeid:%d txnid: %d myself: %d\n",v.nodeid,msg.txn.txnid,n.nodeid)
								committed = false
								break;
							}
						}
					}
					
					if (committed){
						msg_send := Message{msgtype: "request_commit_log", src: n, dest: msg.src, txn:msg.txn}
						// send to all to commit and add to their ledger
						for _,v :=  range n.node_list{
								if(v.Live == 1){
									v.messageQ <- msg_send
								}
						}
						n.commit_logs[msg.txn.txnid]=  &(Log{txn: msg.txn, state: "i dont know", nodes_recieved: make(map[int]bool)})
						//delete(n.pending_logs,msg.txn.txnid)
					}
				} else if msg.msgtype == "request_commit_log" {
					fmt.Printf("[Request_for_commiting_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					//Commit the log
					n.txn_list[n.ledger_entrynum] = *(msg.txn)
					n.ledger_entrynum++;
					// Send ack to the main node
					if(msg.txn.txnid > n.recvd_t[n.nodeid][n.nodeid]){
							n.recvd_t[n.nodeid][n.nodeid]= msg.txn.txnid
						}else if(n.recvd_t[n.nodeid][n.nodeid] == msg.txn.txnid  && n.recvd_n[n.nodeid][n.nodeid] > msg.src.nodeid){
							n.recvd_n[n.nodeid][n.nodeid] = msg.src.nodeid
						}
					msg.src.messageQ <- Message{msgtype: "ack_commit_log", src: n, dest: msg.src, txn:msg.txn}
					
				} else if msg.msgtype == "ack_commit_log" {
					fmt.Printf("[Ack_for_commiting_event_recv] nodeid: %d  txnid: %d  from: %d\n", n.nodeid,msg.txn.txnid,msg.src.nodeid)
					// Check if recieved by all
					log,found  := n.commit_logs[msg.txn.txnid]
					if(!found){
						break
					}
						
					log.nodes_recieved[msg.src.nodeid] = true
					committed := true
					for _,v :=  range n.node_list{
						if(v.Live==1){
							_,found := log.nodes_recieved[v.nodeid]
							if(!found && v.nodeid!=n.nodeid){
								committed = false
								break;
							}
						}
					}
					if(committed){
						fmt.Printf("[ALL_ACKS_FOR_COMMITING] nodeid: %d  txnid: %d \n", n.nodeid, msg.txn.txnid)
						//delete from pending_broadcast only if all have replied committed
						if(msg.txn.txnid > n.recvd_t[n.nodeid][n.nodeid]){
							n.recvd_t[n.nodeid][n.nodeid]= msg.txn.txnid
						}
						delete(n.pending_broadcast,msg.txn.txnid)
						delete(n.commit_logs,msg.txn.txnid)
						delete(n.pending_logs,msg.txn.txnid)
						fmt.Printf("Successful")
					}
					
					// Remove from pending broadcast if recieved by all
				}else if msg.msgtype == "view_change"{
					
					// Update the nodelist accordingly
				}else if msg.msgtype =="request_msg_flushed"{
					
					// Update that msg update from that node
				}else if msg.msgtype =="gossip_share"{
				fmt.Printf("[Gossip_share] nodeid: %d    from: %d\n", n.nodeid,msg.src.nodeid)
					for i := 0; i<n.totalsize; i++ {
						if(n.node_list[i].Live==0){
							continue
						}
						for j := 0; j<n.totalsize; j++{
							if(n.node_list[j].Live==0){
							continue
						}
							if(msg.rcvd_t[i][j]>n.recvd_t[i][j]){
								n.recvd_t[i][j] = msg.rcvd_t[i][j]
							}else if(msg.rcvd_t[i][j]==n.recvd_t[i][j] && msg.rcvd_n[i][j] >n.recvd_n[i][j] ){
								n.recvd_n[i][j] = msg.rcvd_n[i][j]
							}
						}
					}
					// Share your matrix with someone else.
				}else if msg.msgtype =="msg_deliverd" {
					
				}
	    	default:
				continue
		}

		//check and broadcast the txn
		min := 9999999999
		var txn *Transaction
		for k,v := range n.pending_broadcast{
			if(k < min){
				min = k
				txn = v
			}
		}
		if(min != 9999999999){
			_,found :=n.pending_logs[min]
			if (!found){
				n.pending_logs[min] = &(Log{txn: txn, state: "i dont know", nodes_recieved: make(map[int]bool)})
				for i := 0; i<len(n.node_list); i++ {
					if(n.nodeid == n.node_list[i].nodeid){
						continue
					}
					msg := Message{msgtype: "request_event_log", src: n, dest: &n.node_list[i], txn: txn}
					n.node_list[i].messageQ <- msg
				}
			}
		}

	}

	fname := "node" + strconv.Itoa(n.nodeid)+ ".txt"
	f, _ := os.Create(fname)

	for i:=0; i < n.ledger_entrynum; i++{
		f.WriteString("tid: " + strconv.Itoa(n.txn_list[i].txnid)+"  nodeid: "+strconv.Itoa(n.txn_list[i].src.nodeid) + "\n")
	}

	defer f.Close()

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


	if(n.txn_num <= n.max_accepted){
		n.txn_num = n.max_accepted + 1
	}
	//TODO - generate a txn with txnid > max accepted 
	txn := Transaction{src: n, dest: withNode, mod: modNode, amt:amt, txnid: n.maxTxns*n.nodeid+n.txn_num, 
						num_replies:0, aborted:false, waiting_time: 0}
	n.pending_txns[txn.txnid] = &txn

	fmt.Println("[TXN START] txnid",txn.txnid,"  coordinator:",n.nodeid, "  destination: ",withNode.nodeid,"  moderator: ",modNode.nodeid)

	withNode.messageQ <- Message{msgtype: "vote_request", src: n, dest:withNode, txn: &txn}
	modNode.messageQ <- Message{msgtype: "vote_request", src: n, dest:modNode, txn: &txn}
}
