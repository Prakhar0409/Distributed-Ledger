package node


//src id pays dest_id money = amt, the moderating third party is modid/mod
type Transaction struct{
	
	txnid int		//make this unique 
	src *Node 		//pointer to node
	dest *Node 		//pointer to node
	mod *Node 		//pointer to node
	amt int

	//num-replies received by the src.
	num_replies int
	aborted bool
}
