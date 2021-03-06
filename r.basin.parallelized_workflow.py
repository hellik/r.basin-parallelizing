#!/usr/bin/env python

# script for parallelizing r.basin
# assuming the dem in PERMANENT, we create a new mapset for each run
# AUTHOR(S):   Margherita Di Leo, Helmut Kudrnovsky

from multiprocessing import cpu_count,Pool,Lock
import multiprocessing
import os
import sys
import csv

#---
def rbasin_calculation(curr_coors):

  if curr_coors not in coor_list_done:
        current = multiprocessing.current_process()
        mn = current._identity[0]
        print 'running:', mn # check
        
        coor_list_done.append(curr_coors) 
        # this is because the current worker tells the others that 
        # he's taking care of this coordinate pairs

        GISBASE = os.environ['GISBASE'] = "/usr/local/grass-7.1.svn/"
        GRASSDBASE = "/home/v-user/grassdata/"
        MYLOC = "test_location"
  
        # create temp mapset
  
        # set current mapset
        MAPSET = "mymapset"
  
        # build cmd string (r.basin + parameters + curr_coors)
        coor_pairs = curr_coors[0]+","+curr_coors[1]
        prefix = "s_"+curr_coors[0]+"_"+curr_coors[1]
        cmd = "r.basin map=elevation threshold=1000 dir=xxx prefix=%s coordinates=%s" % (prefix, coor_pairs)
  
        # run r.basin
        os.system(cmd)
        
        print "just finished ", curr_coors
 
#---main 
if __name__ == '__main__':

    #initialize coordinates list
    coor_list = [] 
	 
    # open csv file with coor pairs
    with open('coors.csv', 'rb') as coors_csvinfile:
        coorreader = csv.reader(coors_csvinfile, delimiter = ',')
        # parse coor pairs and write them to list
        coor_list = list(coorreader)
    
    # list of coords already done
    coor_list_done = []
     
    pool = Pool(3) # 3 workers (you can increase)
    pool.map(rbasin_calculation, coor_list)
    pool.close()
    pool.join()
