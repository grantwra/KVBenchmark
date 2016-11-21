
#adb -s 0567cbc33443c06b reboot
#sleep 30s
#adb -s 0567cbc33443c06b root
#sleep 2m
i=1

for j in `seq 1 1`;
do
	adb -s 0567cbc33443c06b shell sh /data/removeBenchmarkDatakv.sh
	adb -s 0567cbc33443c06b shell sh /data/preBenchmarkkv.sh #create database 
	adb -s 0567cbc33443c06b shell sh /data/benchmarkkv.sh #run queries
	adb -s 0567cbc33443c06b pull /data/trace.log
	mv trace.log YCSB_WorkloadE_TimingAkv.log
done
