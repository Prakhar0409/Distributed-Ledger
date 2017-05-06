package node


//src id pays dest_id money = amt, the moderating third party is modid
type Transaction struct{
	srcid int
	destid int
	amt int
	modid int
}
