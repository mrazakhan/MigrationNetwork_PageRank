import sys
from string import maketrans
def processFile(fileName, commareplace=False):
	print 'processing file ', fileName
	fout=open('cleaned'+fileName,'w')
	transtab=maketrans(",","\t")
	with open(fileName) as f:
		for line in f:
			if commareplace=="1":
				fout.write(line.translate(None,'()').translate(None,"L").translate(transtab))
			else:
				fout.write(line.translate(None,'()'))	

	fout.close()
	
			

if __name__=='__main__':
	if len(sys.argv)!=2:
		print ' FileName Required as Input Argument'
		sys.exit(-1)

	processFile(sys.argv[1],"1")

