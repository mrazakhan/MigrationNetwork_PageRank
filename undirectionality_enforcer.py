import os
import sys
from collections import defaultdict


def remove_directionality(fin_name, fout_name):
	fin=open(fin_name,'r')
	fout=open(fout_name,'w')

	edges=defaultdict(list)

	for each in fin:
		A,B=each.split('\t')
		A=A.rstrip()
		B=B.rstrip()
		if B  in edges[A] or  A in edges[B]:
			pass
		else:
			edges[A].append(B)

	for k in edges.keys():
		for v in edges[k]:
			fout.write(k+'\t'+v+'\n')

	fin.close()
	fout.close()


if __name__=='__main__':
	print "Required Arguments : InputFileName OutputFileName"
	print 'Passed Arguments'
	print ''+sys.argv[0]+' '+sys.argv[1]+' '+sys.argv[2]
	remove_directionality(sys.argv[1],sys.argv[2])
