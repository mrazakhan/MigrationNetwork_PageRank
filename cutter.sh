#!/bin/bash
#export _JAVA_OPTIONS=-Xmx70G
jar='/export/home/mraza/MNET_DailyCOG/target/scala-2.10/dailycogmain_2.10-0.1.jar'
file_suffix='-Mobility.sor.txt'

input_path='Rwanda_In/RawMobilityFiles/'
output_path='Rwanda_Out/DailyCOG/'
output_suffix='_DailyCOG'

exec_obj_name='DailyCOGMain'


for month in   0601   0602 0603 0604 0605 0606 0607 0608 0609 0610 0611 0612 
#for month in   0606 6067 0608 0609 0610 0611 0612    
#for month in 0502 0503 0504 0505 0506 0507 0508 0509 0510 0511 0512
#0807 0808 0809 0810 0811 0812 
do
	
	cat cleaned${month}_HourlyCOG|cut -d',' -f1,2,3,4,5,6,7,8,9,10 >>./${month}_HourlyCOG.csv
	cat cleaned${month}_HourlyEveningCOG|cut -d',' -f1,2,3,4,5,6,7,8,9,10 >>./${month}_HourlyEveningCOG.csv

done
