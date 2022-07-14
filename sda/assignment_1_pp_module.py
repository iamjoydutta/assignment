"""
This code computes sum.
This program takes input as an array with size n.
It feeds into each processor and each processor 
individually computes sum and return the value.
The main program would return the total sum value
"""

from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
sum = 0

def doParallelProcessing (processId, num_process, l1 = []):
    global sum
    step=1
    num_ele_mismatch_with_num_process = 0
    n = len(l1)
    if processId == 0:
        if n%num_process != 0:
           num_ele_mismatch_with_num_process = n%num_process
        element_per_process = int(n/num_process)
        print("step ",step," : Element_per_process is ",element_per_process)
        if num_process > 1:
             # distribution the portion of array
             # to each child processes to calculate
             # their partial sums
             for i in range (1,num_process):
                index = int(i * element_per_process);
                last_ele_sublist = int(index + element_per_process)
                comm.send(element_per_process, dest=i,tag=1)
                #Make a sublist
                l1_sub = l1[index:last_ele_sublist]
                #Convert list to numpy array
                data = np.array(l1_sub)
                step+=1
                print("step ",step," : sending data", data, " from process ",processId, " to process ",i)
                comm.send(data, dest=i, tag=1)
             #last process adds remaining elements
             index = i * element_per_process
             elements_left = n - index
             comm.send(elements_left, dest=i,tag=1)
             l2_sub = l1[index:elements_left]
             data2 = np.array(l2_sub)
             print("step ",step, " : sending data", data2, " from process ",processId, " to process ",i)
             comm.send(data2, dest=i, tag=1)

        #If size of the process to number of element doesn't match
        #Then process here at Master
        if num_ele_mismatch_with_num_process != 0:
            #print("NUM ELE MISMATCH ",num_ele_mismatch_with_num_process)
            #comm.send(num_ele_mismatch_with_num_process,dest=1,tag=1)
            #l3_sub_list = l1[n-num_ele_mismatch_with_num_process:n]
            step+=1
            print("step ",step," : Computing the sublist ", l1[n-num_ele_mismatch_with_num_process:n], " which were left after equal partitioning")
            for i in range(n - num_ele_mismatch_with_num_process, n):
                sum += l1[i]
            #print("Mismatching data ",l3_sub_list)
            #data3 = np.array(l3_sub_list)
            #comm.send(data3, dest=1, tag=1)

        # master process add its own sublist
        step+=1
        print("step ",step," : Computing the sublist at master ", l1[0:element_per_process])
        for i in range (0,element_per_process):
            sum += l1[i]

        # collects partial sums from other processes
        tmp = 0
        for i in range(1,num_process):
            tmp = comm.recv(source=MPI.ANY_SOURCE, tag=1)
            step+=1
            print("step ",step," : Received partial sum ",tmp , "from slave process to  ",processId)
            sum += tmp
        return sum
    else:
        # Slave Processes
        n_elements_received = comm.recv(source=0,tag=1)
        # Stores the received list segment
        # in local list l2
        l2 = comm.recv(source=0,tag=1)
        step+=1
        print("step ",step," : Recieved at Slave process ",processId, " and data to compute is ",l2)
        #Calculates its partial sum
        partial_sum = 0
        for i in range(0,n_elements_received):
            partial_sum += l2[i]
        # sends the partial sum to the root process
        comm.send(partial_sum,dest=0,tag=1)



def init(l1 = []):
   processID = comm.Get_rank()
   num_process = comm.Get_size()
   #Here we have to feed list for parallel processing
   sum = doParallelProcessing(processID, num_process, l1)
   return sum
