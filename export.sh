export _JAVA_OPTIONS=-Xmx70G
jar='/export/home/mraza/Rwanda_DailyCOG/target/scala-2.10/dailycogmain_2.10-0.1.jar'
file_suffix='-Mobility.sor.txt'
hadoop_outdir_prefix='Rwanda_Out/MonthlyModalCOG/'
hadoop_outdir_suffix='*MonthlyHourlyModal*/part-*'
hadoop_evening_suffix='*MonthlyEveningModal*/part-*'

output_suffix='_HourlyModalCOG'
evening_suffix='_HourlyEveningModalCOG'

export SPARK_JAVA_OPTS+=" -verbose:gc -Xmx15g -Xms1g -XX:-PrintGCDetails -XX:+PrintGCTimeStamps "

#for month in   0703
for month in 0801 0802 0803 0804 0805 0806
do
#        echo "Trying jar $jar file $month$file_suffix ";

#        spark-submit  --class $exec_obj_name --master yarn-client $jar $month$file_suffix --verbose;
#       echo "Executing export hadoop fs -cat $hadoop_outdir_prefix$month$hadoop_outdir_suffix>>$output_path$month$output_suffix "

#       hadoop fs -cat $hadoop_outdir_prefix$month$hadoop_outdir_suffix>>./$month$output_suffix;
#       hadoop fs -cat $hadoop_outdir_prefix$month$hadoop_evening_suffix>>./$month$evening_suffix;

hadoop fs -cat Rwanda_Out/MonthlyModalCOG/$month-Mobility.sor.txt-DailyModal/*>>/data/nas/mraza/ModalCOGFiles/$month-DailyModal.csv
hadoop fs -cat Rwanda_Out/MonthlyModalCOG/$month-Mobility.sor.txt-DailyModalEvening/*>>/data/nas/mraza/ModalCOGFiles/$month-DailyModalEvening.csv
hadoop fs -cat Rwanda_Out/MonthlyModalCOG/$month-Mobility.sor.txt-HourlyEveningModal/*>>/data/nas/mraza/ModalCOGFiles/$month-HourlyEveningModal.csv
hadoop fs -cat Rwanda_Out/MonthlyModalCOG/$month-Mobility.sor.txt-HourlyModal/*>>/data/nas/mraza/ModalCOGFiles/$month-HourlyModal.csv
hadoop fs -cat Rwanda_Out/MonthlyModalCOG/$month-Mobility.sor.txt-MonthlyModal/*>>/data/nas/mraza/ModalCOGFiles/$month-MonthlyModal.csv
hadoop fs -cat Rwanda_Out/MonthlyModalCOG/$month-Mobility.sor.txt-MonthlyModalEvening/*>>/data/nas/mraza/ModalCOGFiles/$month-MonthlyModalEvening.csv

done

