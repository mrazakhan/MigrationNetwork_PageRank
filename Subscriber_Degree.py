import os
import sys
from collections import defaultdict

def degree_calculator(in_name,out_name, sep='\t'):
	fin=open(in_name,'r')
	fout=open(out_name,'w')

	edges=defaultdict(list)

	for each in fin:
		A,B=each.split('\t')
		A,B=A.rstrip(),B.rstrip()
		if B not in edges[A]:
			edges[A].append(B)
		if A not in edges[B]:
			edges[B].append(A)
	for k in edges.keys():
		fout.write(k+','+str(len(edges[k]))+'\n')

	fin.close()
	fout.close()

if __name__=='__main__':
	degree_calculator(sys.argv[1],sys.argv[2])
