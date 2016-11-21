#usage: python newgraph.py 'filename' SQL || BDB

import sys
import matplotlib.pyplot as plt
import numpy as np

filename = sys.argv[1]
#filename2 = 'testing'

#f5 = open(filename2,'w')

reads1 = 0
writes = 0
flushes = 0

start = 0
c_switch = 0
sched_timestamps = []
sched_switches = []
cumulative_time = 0
cumul_time = []
block = {} 
block_op_type = {}
block_insert = 0
block_complete = 0
block_inserted = []
block_insert_timestamps = []
block_completed = []
block_complete_timestamps = []
load_end = 0
load_start = 0

def ret_color(op_type):
#	adder = 1
	if(op_type == 'read'):
#		reads1 = reads1 + adder
#		adder += reads1
#		reads1 = adder
		return 'y'
	if(op_type == 'write sync'):
#		writes += 1
		return 'g'
	if(op_type == 'write sync flush'):
#		flushes += 1
		return 'b'

with open(filename,'r') as log:
	for line in log:
		if('LOAD_END' in line):
                        line = line.strip()
                        columns = line.split()
                        load_end = float(columns[3][:-1]) - load_start
			load_end = load_end * 1000

                if('LOAD_START' in line):
                        line = line.strip()
                        columns = line.split()
                        load_start = float(columns[3][:-1])

		if('START: App' in line):
			columns = line.split()
			pid = columns[0].split('-')[1]
		elif(sys.argv[2]+'_START' in line):
			line = line.strip()
			columns = line.split()
			start_time = columns[3][:-1]
			start = float(start_time)*1000
			cumul_time.append(0);

		#	f = open(filename2,'w')
#			f5.write(line + ' ' + str(0) + '\n')
		#	f.close()

		elif(sys.argv[2]+'_END' in line):
			line = line.strip()
			columns = line.split()
			end_time = columns[3][:-1]
			break
'''		
		elif('LOAD_END' in line):
                        line = line.strip()
                        columns = line.split()
                        load_end = columns[3][:-1] - load_start

		elif('LOAD_START' in line):
                        line = line.strip()
                        columns = line.split()
                        load_start = columns[3][:-1]
'''

with open(filename,'r') as log:
	while(not(start_time in log.readline())):
		pass
	line = log.readline()
	while(not(end_time in line)):
		if(pid in line and 'sched_switch' in line):
			line = line.strip()
			columns = line.split()
			c_switch += 1
			sched_timestamps.append(float(columns[3][:-1])*1000)

#			f5.write(line + ' ' + str(float(columns[3][:-1])*1000) + '\n')

			if(('next_pid='+pid) in line):
				start = sched_timestamps[-1]
				cumul_time.append(cumul_time[-1])

				#f = open(filename2,'w')
#				f5.write(line + ' ' + str(cumul_time[-1]) + '\n')
				#f.close()

				sched_switches.append(0)
			else:
				cumulative_time = cumulative_time + sched_timestamps[-1] - start
				cumul_time.append(cumulative_time)

				#f = open(filename2,'w')
#				f5.write(line + ' ' + str(cumulative_time) + '\n')
				#f.close()

				sched_switches.append(1)	
		if(pid in line and 'block_rq_insert' in line):
		#if('block_rq_insert' in line):
			line = line.strip()
			columns = line.split()
			block_insert += 1
			block_inserted.append(block_insert)
			block_insert_timestamps.append(float(columns[3][:-1])*1000)
			block[(columns[9]+' + '+columns[11])] = [block_insert_timestamps[-1]]
			op_type = ''	
			if('R' in columns[6]):
				op_type += 'read '
			if('W' in columns[6]):
				op_type +=  'write '
			if('S' in columns[6]):
				op_type += 'sync '
			if('F' in columns[6]):
				op_type += 'flush '
			block_op_type[(columns[9]+' + '+columns[11])] = op_type.strip()
		elif('block_rq_complete' in line):
			line = line.strip()
			columns = line.split()
			if((columns[8]+' + '+columns[10]) in block):
				block_complete += 1
				block_completed.append(block_complete)
				block_complete_timestamps.append(float(columns[3][:-1])*1000)
				block[(columns[8]+' + '+columns[10])].append(block_complete_timestamps[-1]);

		line = log.readline()
	end=float(line.split()[3][:-1])

#fig, (ax1, ax2) = plt.subplots(2,1,sharex = True)

sched_timestamps = np.array(sched_timestamps) - float(start_time)*1000
cumul_time = np.array(cumul_time)  
cumul_time = np.append(cumul_time, cumulative_time + float(end_time)*1000 - start)

block_insert_timestamps = np.array(block_insert_timestamps) - float(start_time)*1000
block_complete_timestamps = np.array(block_complete_timestamps) - float(start_time)*1000
#x_range = [0,float(end_time)*1000 - float(start_time)*1000]
#plt.xlim(x_range)
#ax1.set_title('DB Operations Over Length of Trace (' + sys.argv[2] + ')')
sched_timestamps = np.insert(sched_timestamps,0,0)
sched_switches = np.insert(sched_switches,0,0)
sched_timestamps = np.append(sched_timestamps,float(end_time)*1000 - float(start_time)*1000)
sched_switches = np.append(sched_switches,1)

#ax1.step(sched_timestamps,sched_switches,'k',label='')
#ax1.step(sched_timestamps,sched_timestamps,'k',label='')
#ax1.set_ylim((0,2))
#ax1.set_ylabel('Number of Writes')

block_insert_timestamps = np.insert(block_insert_timestamps,0,0)
block_inserted = np.insert(block_inserted,0,0)
block_complete_timestamps = np.insert(block_complete_timestamps,0,0)
block_completed = np.insert(block_completed,0,0)
'''
ax2.set_title('block_operations')
ax2.plot(block_insert_timestamps,block_inserted,'o',label='')
ax2.plot(block_insert_timestamps,block_inserted,'k',label='block operation start event')
ax2.plot(block_complete_timestamps,block_completed,'o',label='')
ax2.plot(block_complete_timestamps,block_completed,'k',label='block operation end event')
ax2.set_ylabel('number of block operations')
legends2 = ax2.legend(loc='lower right')
'''

#total_io = 0;
count = 1
reads1 = 0
writes = 0
flushes = 0
for key,block_op in sorted(block.items(), key=lambda t:t[1][0]):
	#count += 0.25
	#count += 1
	op_type = block_op_type[key]
	block_op = np.array(block_op) - float(start_time)*1000

	if 'read' in op_type:
		reads1 += 1

	if 'write sync' in op_type:
		writes += 1

	if 'write sync flush' in op_type:
		flushes += 1

	#ax1.hlines(count,block_op[0],block_op[0],ret_color(op_type),lw='10.5',label=op_type)
	#ax1.vlines(block_op[0],count+0.05,count-0.25,ret_color(op_type),lw='10',label='')
	#ax1.vlines(block_op[0],count+0.05,count-0.25,ret_color(op_type),lw='10',label='')
	count += 1

total_io = count
#ax1.set_ylabel('Number of Writes (' + str(count) + ' Total)')
#handles, labels = ax1.get_legend_handles_labels()
#i = 1 
#while i<len(labels):
#	if(labels[i] in labels[:i]):
#		del(labels[i])
#		del(handles[i])
#	else:
#		i += 1
#legends = ax1.legend(handles,labels,loc='upper left')
#ax1.set_ylim((0,count+1))

#ax2.set_title('Trace Time')
#ax2.plot(sched_timestamps,cumul_time)
#ax2.set_ylabel('Time Spent on Ops (ms)')
#fig.suptitle(sys.argv[1])
#fig.text(0.5, 0.04, 'Total Runtime(in ms)', ha='center', va='center')

workload = ''
if 'A' in sys.argv[1]:
        workload = 'A'
if 'B_T' in sys.argv[1]:
        workload = 'B'
if 'C_' in sys.argv[1]:
        workload = 'C'
if 'D' in sys.argv[1]:
        workload = 'D'
if 'E' in sys.argv[1]:
        workload = 'E'
if 'F' in sys.argv[1]:
        workload = 'F'
if 'IA' in sys.argv[1]:
        workload = 'IA'
if 'IB' in sys.argv[1]:
        workload = 'IB'
if 'IC' in sys.argv[1]:
        workload = 'IC'


filename2 = 'info/' + workload + '_' + sys.argv[2]
f = open(filename2,'w')
runtime = 0.0
for run in sched_timestamps:
	if float(run) > runtime:
		runtime = float(run)
f.write('Runtime: ' + str(runtime) + '\n')

cpu_time = 0.0
for cpu in cumul_time:
	if float(cpu) > cpu_time:
		cpu_time = float(cpu)

f.write('CPU Time: ' + str(cpu_time) + '\n')

f.write('Load End Time: ' + str(load_end) + '\n')

total_io = reads1 + writes + flushes
f.write('Total Number of IO Calls: ' + str(total_io) + '\n')

f.write('Number of Reads: ' + str(reads1) + '\n')

f.write('Number of Writes: ' + str(writes) + '\n')

f.write('Number of Flushes: ' + str(flushes) + '\n')

f.close()


#f5.close()

#plt.show()

