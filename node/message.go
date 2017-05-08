package node


/*type_num vs msgtype
 1 -> join request
 2 -> join accept
 3 -> view change request
 4 -> liveliness message - "I'm alive!"
 5 -> ping message (in case required)
 6 -> txn_init
 7 -> vote_request
 8 -> vote_commit
 9 -> vote_abort
 10 -> global commit
 11 -> global abort
*/
type Message struct{
	
	msgtype string
	src *Node 								//pointer to source node
	dest *Node 								//pointer to destination node

	//2-phase commit shit
	txn *Transaction						//pointer to transaction 

	//  join request => type = 1 
	join_nodeid int

	// join accept => type = 2
		//todo - send the src's ledger - idk

	//....
}
