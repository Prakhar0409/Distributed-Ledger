import os
import sys

if len(sys.argv) != 2:
	print("Usage: python checker.py <num_nodes>")
	sys.exit()


num_nodes = int(sys.argv[1])

base = "tests/node"

fname0 = base + "0.txt"

f0 = open(fname0,'r')
con0 = f0.readlines()
f0.close()

longest = 0
for i in range(1,num_nodes):
	fname1 = base+str(i)+".txt"
	f1 = open(fname1,'r')
	con1 = f1.readlines()
	f1.close()

	### list 0 is the biggest
	if len(con0) < len(con1):
		tmp = con0
		con0 = con1
		con1 = tmp
		longest = i

	for j in range(0,len(con1)):
		if con0[j] != con1[j] :
			print("Panic: longest list till now doesn't match with node"+str(i)+"   line:"+str(j))
		
print("Node "+str(longest)+" has longest txn")
print("Wink! Wink wink!! That ends semester for you babes ;)")




