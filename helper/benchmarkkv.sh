trace_dir=/sys/kernel/debug/tracing

sleep 15
sync
echo 3 > /proc/sys/vm/drop_caches

echo 150000 > $trace_dir/buffer_size_kb

echo 1 > $trace_dir/events/sched/sched_switch/enable
#echo 1 > $trace_dir/events/block/block_rq_insert/enable
#echo 1 > $trace_dir/events/block/block_rq_complete/enable

echo > $trace_dir/trace
echo 1 > $trace_dir/tracing_on

#am kill-all
am start -n com.example.kvbenchmark/com.example.kvbenchmark.MainActivity
#sleep 4000
sleep 76

echo 0 > $trace_dir/tracing_on
cat $trace_dir/trace > /data/trace.log
echo 1500 > $trace_dir/buffer_size_kb
echo 0 > $trace_dir/events/sched/sched_switch/enable
echo 0 > $trace_dir/events/block/block_rq_insert/enable
echo 0 > $trace_dir/events/block/block_rq_complete/enable
