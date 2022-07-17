"""
This code computes sum.
This program takes input as an array with size n.
It feeds into each processor and each processor
individually computes sum and return the value.
The main program would return the total sum value
"""

from mpi4py import MPI
import numpy as np
import pandas as pd
import time

comm = MPI.COMM_WORLD
processId = comm.Get_rank()
num_process = comm.Get_size()
step = 1
total_transactions = 0
units_sold_per_region = {}
profit_per_region = {}


def calculate_sum_parallel(l1=[]):
	calculated_sum = 0
	global step
	num_ele_mismatch_with_num_process = 0
	n = len(l1)
	if processId == 0:
		if n % num_process != 0:
			num_ele_mismatch_with_num_process = n % num_process
		element_per_process = int(n / num_process)
		print("step ", step, " : Element_per_process is ", element_per_process)
		if num_process > 1:
			# distribution the portion of array
			# to each child processes to calculate
			# their partial sums
			for i in range(1, num_process):
				index = int(i * element_per_process)
				last_ele_sublist = int(index + element_per_process)
				# print("sending elements_per_process, ", element_per_process, " to process ", i)
				comm.send(element_per_process, dest=i, tag=1)
				# Make a sublist
				l1_sub = l1[index:last_ele_sublist]
				# Convert list to numpy array
				input_list_as_array = np.array(l1_sub)
				step += 1
				print("step ", step, " : sending data from process ", processId, " to process ", i)
				comm.send(input_list_as_array, dest=i, tag=1)

		# If size of the process to number of element doesn't match
		# Then process here at Master
		if num_ele_mismatch_with_num_process != 0:
			step += 1
			print("step ", step, " : Computing the sublist which were left after equal partitioning")
			for i in range(n - num_ele_mismatch_with_num_process, n):
				calculated_sum += l1[i]

		# master process add its own sublist
		step += 1
		print("step ", step, " : Computing the sublist at master ")
		for i in range(0, element_per_process):
			calculated_sum += l1[i]
		print("total sum after Computing the sublist at master : ", calculated_sum)

		# collects partial sums from other processes
		tmp = 0
		# print("Expecting reply from ", num_process-1, " slaves")
		for process_num in range(1, num_process):
			tmp = comm.recv(source=MPI.ANY_SOURCE, tag=1)
			step += 1
			print("step ", step, " : Received partial sum ", tmp, "from slave process to  ", processId)
			calculated_sum += tmp
			tmp = 0
		# print("returning total sum : ", calculated_sum)
		return calculated_sum


if processId == 0:
	master_start_time = time.time()
	data = pd.read_csv("../geosales.csv")
	total_transactions = len(data.index)
	# Partition data based on region
	partitioned_data = data.groupby('region')
	for data in partitioned_data:
		units_sold_per_region[data[0]] = calculate_sum_parallel(data[1]['units_sold'].to_list())
		profit_per_region[data[0]] = calculate_sum_parallel(data[1]['total_profit'].to_list())
		print("Completed processing region : ", data[0], " units_sold : ", units_sold_per_region,
			  " total_profits_per_region : ", profit_per_region)
	print(units_sold_per_region)
	print(profit_per_region)
	total_profit = calculate_sum_parallel(list(profit_per_region.values()))
	print("Total Profit is : ", total_profit)
	print("Average profit per transaction is : ", (total_profit / total_transactions))
	# signal to slaves to terminate
	# print("Number of Processes ", num_process)
	for j in range(1, num_process):
		print("sending Terminate to process ", j)
		comm.send(0, dest=j, tag=1)
		resp = comm.recv(source=MPI.ANY_SOURCE, tag=1)
		# print("Response ", resp)
	master_end_time = time.time()
	print("Time taken by master process : ", master_end_time - master_start_time)
	# print("Done with master")
else:
	# Slave Processes
	slave_start_time = time.time()
	while True:
		# print("Slave ", processId, " waiting for data")
		n_elements_received = comm.recv(source=0, tag=1)
		if n_elements_received == 0:
			# print("Slave ", processId, " Exiting")
			slave_end_time = time.time()
			print("Time taken by Slave process", processId, "  : ", slave_end_time - slave_start_time)
			comm.send("done", dest=0, tag=1)
			break
		# print("Slave received number of elements, ", n_elements_received, " process ", processId)
		# Stores the received list segment
		# in local list l2
		l2 = comm.recv(source=0, tag=1)
		step += 1
		print("step ", step, " : Received at Slave process ", processId, " data to compute ")
		# Calculates its partial sum
		partial_sum = 0
		if len(l2) > 0:
			for elm_rec_index in range(0, len(l2)):
				partial_sum += l2[elm_rec_index]
		# sends the partial sum to the root process
		# print("Sending partial sum ", partial_sum, " from slave process ", processId, " to master")
		comm.send(partial_sum, dest=0, tag=1)
		partial_sum = 0
		# print("Done from Slave!")
# print("Exit!!!!!")
