#!/bin/bash
#export _JAVA_OPTIONS=-Xmx70G
jar='/export/home/mraza/MNET_PageRank/target/scala-2.10/pagerankex_2.10-0.1.jar'
file_suffix='-Mobility.sor.txt'

input_path='Rwanda_In/RawMobilityFiles/'
output_path='Rwanda_Out/MonthlyModalCOG/'
output_suffix='_DailyCOG'

exec_obj_name='PageRankCalculator'


for month in  0604 #0808 0809 0810 0811 0812 0706 0705 0704 0703 0702 0701 0708 0707  0709 0710 0711 0712    
#0807 0808 0809 0810 0811 0812 
do
   	echo "Trying jar $jar file $input_path$month$file_suffix ";
	
	spark-submit --class $exec_obj_name \
	--master yarn --driver-memory 38G \
	--executor-memory 64G \
	--executor-cores 32 \
	--num-executors 3 \
	 --verbose $jar $month;
	#spark-submit --class $exec_obj_name --master yarn-client --driver-memory 96G --master local[24]  --verbose $jar $month$file_suffix $input_path $output_path;


done
