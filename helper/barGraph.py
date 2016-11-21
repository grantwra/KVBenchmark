#Usage: python barGraph.py (CPU,RUN,IO) (workload)
#Ex. python barGraph.py CPU A

import sys
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.patches as mpatches

path_to_default = 'configs/defaultSQLBDB/info/' #sql and bdb
path_to_bdb100_cache = 'configs/BDB100MBCache/info/' #bdb
#path_to_heap25 = 'configs/heap25SQLBDB/info/' #sql and bdb
path_to_heap50 = 'configs/heap50SQLBDB/info/' #sql and bdb
path_to_heap25 = 'configs/heap25SQLBDB/info/' #sql and bdb
path_to_h2_after_every = 'configs/H2AfterEveryOp/info/' #h2
path_to_h2_after_burst = 'configs/H2AfterEveryBurst/info/' #h2
path_to_h2_after_end = 'configs/H2AfterEnd/info/' #h2

default_sql = path_to_default + sys.argv[2] + '_SQL'
default_bdb = path_to_default + sys.argv[2] + '_BDB'
bdb100 = path_to_bdb100_cache + sys.argv[2] + '_BDB'
#heap25_sql = path_to_heap25 + sys.argv[2] + '_SQL'
#heap25_bdb = path_to_heap25 + sys.argv[2] + '_BDB'
heap50_sql = path_to_heap50 + sys.argv[2] + '_SQL'
heap50_bdb = path_to_heap50 + sys.argv[2] + '_BDB'
heap25_sql = path_to_heap25 + sys.argv[2] + '_SQL'
heap25_bdb = path_to_heap25 + sys.argv[2] + '_BDB'
h2_every = path_to_h2_after_every + sys.argv[2] + '_H2'
h2_burst = path_to_h2_after_burst + sys.argv[2] + '_H2'
h2_end = path_to_h2_after_end  + sys.argv[2] + '_H2'

fileList = []

fileList.append(default_sql)
fileList.append(default_bdb)
fileList.append(bdb100)
#fileList.append(heap25_sql)
#fileList.append(heap25_bdb)
fileList.append(heap50_sql)
fileList.append(heap50_bdb)
fileList.append(heap25_sql)
fileList.append(heap25_bdb)
fileList.append(h2_every)
fileList.append(h2_burst)
fileList.append(h2_end)

cpu_list = []
run_list = []
io_list = []
read_list = []
write_list = []
flush_list = []

def findname(i):
	if i == 0:
		return 'SQL'
        if i == 1:
                return 'BDB'
        if i == 2:
                return 'BDB100'
	if i == 3:
		return 'Heap50SQL'
        if i == 4:
                return 'Heap50BDB'
        if i == 5:
                return 'Heap25SQL'
        if i == 6:
                return 'Heap25BDB'
        if i == 7:
                return 'H2 Every Op'
        if i == 8:
                return 'H2 Every Burst'
        if i == 9:
                return 'H2 End'


if sys.argv[1] == 'CPU':

	for name in fileList:
		with open(name,'r') as log:
			for line in log:
				#print line
				if 'CPU' in line:
					column = line.split(':')
					num = column[1].replace(' ','')
					numFinal = num.replace('\n','')
					#print numFinal
					cpu_list.append(float(numFinal))

	#print 'goodJOb!'
	#for x in cpu_list:
	#	print x
	N = 10
	ind = np.arange(N)
	fig, ax = plt.subplots()
	barlist = ax.bar(ind,cpu_list,1, alpha = 0.5)
	ax.bar(0,0,1,alpha = 0.5, color = 'g',label = 'Best Case')
	ax.bar(0,0,1,alpha = 0.5, color = 'r', label = 'Worst Case')
	ax.bar(0,0,1,alpha = 0.5, label = 'Everything Else')
	#barlist[8].set_color('r')
	ax.set_title('CPU time for YCSB Workload ' + sys.argv[2])
	ax.set_ylabel('CPU Time (ms)')
	#plt.xticks(rotation='vertical')
	ax.set_xticks(ind + 0.5)
	
	from textwrap import wrap
	labels = [ 'Default SQL', 'Default BDB', 'BDB 100MB Cache', 'SQL heap restricted to 50%', 'BDB heap restricted to 50%', 'SQL heap restricted to 25%','BDB heap restricted to 25%' , 'H2 with sync after every query', 'H2 with sync after every burst','H2 with sync at end' ]
	labels = [ '\n'.join(wrap(l, 10)) for l in labels ]
	ax.set_xticklabels(labels)

#	ax.set_xticklabels(('Default SQL', 'Default BDB', 'BDB 100MB Cache', 'SQL50%', 'G5'))
	manager = plt.get_current_fig_manager()
	manager.resize(*manager.window.maxsize())
	#plt.tight_layout()

	bestint = 0
	worstint = 0
	counter = 0
	worst = 0.0
	best = 500000000000000.0
	for i in cpu_list:
		if i > worst:
			worst = i
			worstint = counter
		if i < best:
			best = i
			bestint = counter
		counter += 1

	barlist[bestint].set_color('g')
	barlist[worstint].set_color('r')
	plt.legend(loc='upper left')

	fig15 = plt.gcf()

	#fig15.savefig('/Graphs/' + sys.argv[1] + '_' + sys.argv[2])

	plt.show()
	fig15.savefig('Graphs/' + sys.argv[1] + '_' + sys.argv[2])
	
	saveinfo = 'Graphs/info/' + sys.argv[1] + '_' + sys.argv[2]
	best1 = 5000000.0
	bestint1 = 0
	secondbest = 5000000.0
	secondbestint = 0
	counter1 = 0
	for j in cpu_list:
		if j < best1:
			#secondbest = best1
			#secondbestint = bestint1
			best1 = j
			bestint1 = counter1
		#if j > best1:
		#	if j < secondbest:
		#		secondbest = j
		#		secondbestint = counter1
		counter1 += 1
	counter2 = 0
	for j2 in cpu_list:
		if j2 < secondbest:
			if j2 > best1:
				secondbest = j2	
				secondbestint = counter2
		counter2 += 1

	bestname = findname(bestint1)
	secondbestname = findname(secondbestint)

	f2 = open(saveinfo,'w')
	f2.write('1. ' + bestname + ':' + str(best1) + '\n')
	f2.write('2. ' + secondbestname + ':' + str(secondbest) + '\n')
	f2.close()

else: 
	if sys.argv[1] == 'RUN':
		#print 'RunJObb!!'
		for name in fileList:
			with open(name,'r') as log:
                        	for line in log:
                                	#print line
                                	if 'Runtime' in line:
						column = line.split(':')
                                        	num = column[1].replace(' ','')
                                	        numFinal = num.replace('\n','')
                        	                #print numFinal
                	                        run_list.append(float(numFinal))

        	#print 'goodJOb!'
        	#for x in cpu_list:
        	#       print x
        	N = 10
        	ind = np.arange(N)
        	fig, ax = plt.subplots()
        	barlist = ax.bar(ind,run_list,1, alpha = 0.5)
		ax.bar(0,0,1,alpha = 0.5, color = 'g',label = 'Best Case')
	        ax.bar(0,0,1,alpha = 0.5, color = 'r', label = 'Worst Case')
        	ax.bar(0,0,1,alpha = 0.5, label = 'Everything Else')

	        #barlist[8].set_color('r')
		ax.set_title('Total Runtime for YCSB Workload ' + sys.argv[2])
	        ax.set_ylabel('Runtime (ms)')
        	#plt.xticks(rotation='vertical')
        	ax.set_xticks(ind + 0.5)

        	from textwrap import wrap
        	labels = [ 'Default SQL', 'Default BDB', 'BDB 100MB Cache', 'SQL heap restricted to 50%', 'BDB heap restricted to 50%', 'SQL heap restricted to 25%','BDB heap restricted to 25%' , 'H2 with sync after every query', 'H2 with sync after every burst','H2 with sync at end' ]
        	labels = [ '\n'.join(wrap(l, 10)) for l in labels ]
        	ax.set_xticklabels(labels)

#       	ax.set_xticklabels(('Default SQL', 'Default BDB', 'BDB 100MB Cache', 'SQL50%', 'G5'))
        	manager = plt.get_current_fig_manager()
        	manager.resize(*manager.window.maxsize())
        	#plt.tight_layout()

        	bestint = 0
        	worstint = 0
        	counter = 0
        	worst = 0.0

		best = 500000000000000.0
	        for i in run_list:
        	        if i > worst:
                	        worst = i
                        	worstint = counter
                	if i < best:
                        	best = i
                        	bestint = counter
                	counter += 1

        	barlist[bestint].set_color('g')
        	barlist[worstint].set_color('r')

		plt.legend(loc='upper left')

		fig15 = plt.gcf()
        	plt.show()
		fig15.savefig('Graphs/' + sys.argv[1] + '_' + sys.argv[2])

		saveinfo = 'Graphs/info/' + sys.argv[1] + '_' + sys.argv[2]
	        best1 = 5000000.0
        	bestint1 = 0
       		secondbest = 5000000.0
        	secondbestint = 0
        	counter1 = 0
        	for j in run_list:
                	if j < best1:
                        	#secondbest = best1
                        	#secondbestint = bestint1
                        	best1 = j
                        	bestint1 = counter1
                	#if j > best1:
                #       	if j < secondbest:
                #               	secondbest = j
                #               	secondbestint = counter1
                	counter1 += 1
        	counter2 = 0
        	for j2 in run_list:
                	if j2 < secondbest:
                        	if j2 > best1:
                                	secondbest = j2
                                	secondbestint = counter2
                	counter2 += 1

        	bestname = findname(bestint1)
        	secondbestname = findname(secondbestint)

        	f2 = open(saveinfo,'w')
        	f2.write('1. ' + bestname + ':' + str(best1) + '\n')
        	f2.write('2. ' + secondbestname + ':' + str(secondbest) + '\n')
        	f2.close()


	else:
		if sys.argv[1] == 'IO':
			#print 'IObruh!'
			for name in fileList:
		                with open(name,'r') as log:
                		        for line in log:
                                		#print line
                                		if 'IO' in line:
                                        		column = line.split(':')
                                        		num = column[1].replace(' ','')
                                        		numFinal = num.replace('\n','')
                                        		#print numFinal
                                        		io_list.append(float(numFinal))
						if 'Reads' in line:
							column = line.split(':')
                                                        num = column[1].replace(' ','')
                                                        numFinal = num.replace('\n','')
                                                        #print numFinal
                                                        read_list.append(float(numFinal))
						if 'Writes' in line:
							column = line.split(':')
                                                        num = column[1].replace(' ','')
                                                        numFinal = num.replace('\n','')
                                                        #print numFinal
                                                        write_list.append(float(numFinal))
						if 'Flushes' in line:
							column = line.split(':')
                                                        num = column[1].replace(' ','')
                                                        numFinal = num.replace('\n','')
                                                        #print numFinal
               	                                        flush_list.append(float(numFinal))
			#print io_list[0]
			#print read_list[0]
			#print write_list[0]
			#print flush_list[0]
			N = 10
	                ind = np.arange(N)
        	        fig, ax = plt.subplots()
                	#barlist = ax.bar(ind,io_list,1, alpha = 0.5)
                #barlist[8].set_color('r')
			width = 0.25
        	        ax.bar(ind,flush_list,width,color='b',label='Flushes')
			ax.bar(ind + width, write_list,width,color='g',label='Writes')
			ax.bar(ind + width + width, read_list,width,color='y',label='Reads')
			ax.bar(ind + width + width + width, io_list, width,color='#551a8b',label='Total IO Events')
			ax.set_title('Total IO events for YCSB Workload ' + sys.argv[2])
                	ax.set_ylabel('Number of IO Events')
                #plt.xticks(rotation='vertical')
                	ax.set_xticks(ind + 0.5)

                	from textwrap import wrap
                	labels = [ 'Default SQL', 'Default BDB', 'BDB 100MB Cache', 'SQL heap restricted to 50%', 'BDB heap restricted to 50%', 'SQL heap restricted to 25%','BDB heap restricted to 25%' , 'H2 with sync after every query', 'H2 with sync after every burst','H2 with sync at end' ]
                	labels = [ '\n'.join(wrap(l, 10)) for l in labels ]
                	ax.set_xticklabels(labels)

#               	ax.set_xticklabels(('Default SQL', 'Default BDB', 'BDB 100MB Cache', 'SQL50%', 'G5'))
                	manager = plt.get_current_fig_manager()
                	manager.resize(*manager.window.maxsize())
                #	plt.tight_layout()

			plt.legend(loc='upper left')
			fig15 = plt.gcf()
                	plt.show()
			fig15.savefig('Graphs/' + sys.argv[1] + '_' + sys.argv[2])

			saveinfo = 'Graphs/info/' + sys.argv[1] + '_' + sys.argv[2]
			bestio = 500000000
			secondbestio = 5000000000
			bestreads = 50000000000
			secondbestreads = 500000000
			bestwrites = 50000000000
			secondbestwrites = 500000000
			bestflushes = 5000000000
			secondbestflushes = 50000000
			bestioint = 0
			secondbestioint = 0
			bestreadsint = 0
			secondbestreadsint = 0
			bestwritesint = 0
			secondbestwritesint = 0
			bestflushesint = 0
			secondbestflushesint = 0

			counter = 0
			for io in io_list:
				if io < bestio:
					bestio = io
					bestioint = counter
				counter += 1

			counter = 0
			for io2 in io_list:
				if io2 <= secondbestio:
					if counter != bestioint:
						secondbestio = io2
						secondbestioint = counter
				counter += 1

			counter = 0
			for read in read_list:
				if read < bestreads:
					bestreads = read
					bestreadsint = counter
				counter += 1

			counter = 0
			for read2 in read_list:
				if read2 <= secondbestreads:
					if counter != bestreadsint:
						secondbestreads = read2
						secondbestreadsint = counter
				counter += 1
		
			counter = 0
                        for write in write_list:
                                if write < bestwrites:
                                        bestwrites = write
                                        bestwritesint = counter
                                counter += 1

                        counter = 0
                        for write2 in write_list:
                                if write2 <= secondbestwrites:
                                        if counter != bestwritesint:
                                                secondbestwrites = write2
                                                secondbestwritesint = counter
                                counter += 1

			counter = 0
                        for flush in flush_list:
                                if flush < bestflushes:
					if flush != 0:
                                        	bestflushes = flush
                                        	bestflushesint = counter
                                counter += 1

                        counter = 0
                        for flush2 in flush_list:
                                if flush2 <= secondbestflushes:
                                        if counter != bestflushesint:
						if flush2 != 0:
                                                	secondbestflushes = flush2
                                                	secondbestflushesint = counter
                                counter += 1
	
			bestioname = findname(bestioint)
			secondbestioname = findname(secondbestioint)
			bestreadname = findname(bestreadsint)
			secondbestreadname = findname(secondbestreadsint)
			bestwritename = findname(bestwritesint)
                        secondbestwritename = findname(secondbestwritesint)
			bestflushname = findname(bestflushesint)
                        secondbestflushname = findname(secondbestflushesint)


			f = open(saveinfo, 'w')
			f.write('Total IO 1. ' + bestioname + ':' + str(bestio) + '\n')
			f.write('Total IO 2. ' + secondbestioname + ':' + str(secondbestio) + '\n')
			f.write('Reads 1. ' + bestreadname + ':' + str(bestreads) + '\n')
                        f.write('Reads 2. ' + secondbestreadname + ':' + str(secondbestreads) + '\n')
			f.write('Writes 1. ' + bestwritename + ':' + str(bestwrites) + '\n')
                        f.write('Writes 2. ' + secondbestwritename + ':' + str(secondbestwrites) + '\n')
			f.write('Flushes 1. ' + bestflushname + ':' + str(bestflushes) + '\n')
                        f.write('Flushes 2. ' + secondbestflushname + ':' + str(secondbestflushes) + '\n')
			f.close()


		else:
			print 'wrong input :('
