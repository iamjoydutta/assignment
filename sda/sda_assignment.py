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

comm = MPI.COMM_WORLD
processId = comm.Get_rank()
num_process = comm.Get_size()


def do_parallel_processing(l1=[], calculated_sum=0):
	step = 1
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
				index = int(i * element_per_process);
				last_ele_sublist = int(index + element_per_process)
				comm.send(element_per_process, dest=i, tag=1)
				# Make a sublist
				l1_sub = l1[index:last_ele_sublist]
				# Convert list to numpy array
				input_list_as_array = np.array(l1_sub)
				step += 1
				print("step ", step, " : sending data from process ", processId, " to process ", i)
				comm.send(input_list_as_array, dest=i, tag=1)
			# last process adds remaining elements
			index = i * element_per_process
			elements_left = n - index
			comm.send(elements_left, dest=i, tag=1)
			l2_sub = l1[index:elements_left]
			data2 = np.array(l2_sub)
			print("step ", step, " : sending data from process ", processId, " to process ", i)
			comm.send(data2, dest=i, tag=1)

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
		for i in range(1, num_process):
			tmp = comm.recv(source=MPI.ANY_SOURCE, tag=1)
			step += 1
			print("step ", step, " : Received partial sum ", tmp, "from slave process to  ", processId)
			calculated_sum += tmp
		print("returning total sum : ", calculated_sum)
		return calculated_sum
	else:
		# Slave Processes
		n_elements_received = comm.recv(source=0, tag=1)
		# Stores the received list segment
		# in local list l2
		l2 = comm.recv(source=0, tag=1)
		step += 1
		print("step ", step, " : Recieved at Slave process ", processId, " and data to compute is ", l2)
		# Calculates its partial sum
		partial_sum = 0
		if len(l2) > 0:
			for i in range(0, n_elements_received):
				partial_sum += l2[i]
		# sends the partial sum to the root process
		comm.send(partial_sum, dest=0, tag=1)


if __name__ == '__main__' and processId == 0:
	data = pd.read_csv("../geosales.csv")
	no_of_transactions = len(data.index)
	# Partition data based on region
	partitioned_data = data.groupby('region')
	region_wise_count = {}
	total_profits_per_region = {}
	for data in partitioned_data:
		region_wise_count[data[0]] = do_parallel_processing(data[1]['units_sold'].to_list(), 0)
		total_profits_per_region[data[0]] = do_parallel_processing(data[1]['total_profit'].to_list(), 0)
		print("Completed processing region : ", data[0], " units_sold : ", region_wise_count,
			" total_profits_per_region : ", total_profits_per_region)

	print(region_wise_count)
	print(total_profits_per_region)
	total_profit = do_parallel_processing(list(total_profits_per_region.values()), 0)
	print("Total Profit is : ", total_profit)
	print("Average profit per transaction is : ", (total_profit / no_of_transactions))
