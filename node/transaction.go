package node


//src id pays dest_id money = amt, the moderating third party is modid/mod
type Transaction struct{
	txnid int		//make this unique 
	src Node
	dest Node
	amt int
	mod Node

	//num-replies received by the src.
	num_replies int
	aborted bool
	pending bool
}
