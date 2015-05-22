#/bin/sh

for f in `ls EdgeList*.csv`
do
echo 'Processing  '$f
	python cleaner.py $f
done
