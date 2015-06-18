#!/bin/env python
####################
#author:fmoscato
#script per convertire file csv in json
#Lo script legge un template json.
#Come input prende un csv di input ed un file di output 
###################
from ConfigParser import ConfigParser
import string 
import os
import os.path
import csv
import json
import time
from decimal import *

#used to resolve the configuration file outside the directory
dir=os.path.dirname(os.path.realpath(__file__))

Usage="""
python csv2json.py

options:

--input_file    csv di input 
--output_file   file di output dove scrivere json

"""



class   ConfigFile(ConfigParser):
    def __init__(self,  path=None):
        ConfigParser.__init__(self)
        self.read(path)   


def str2bool(v):
  return v.lower() in ("true", "1")


def clean(j):
    #remove all the empty 
    keys_toremove=[]
    #remove the more complex structure
    for k,v in j.iteritems():
       if k=='oP' and v[0] ==0.0:
          keys_toremove.append(k)
       if k=='sP2' and v[4] ==0.0:
          keys_toremove.append(k)
       if k=='vP' and v[4] == 'NULL':
          keys_toremove.append(k)
       if k=='Alb' and v[0]==0.0:
          keys_toremove.append(k)
       if k=='nc' and v>1:
          keys_toremove.append(k) 
    return dict((k, v) for k, v in j.iteritems() if k not in keys_toremove)



def main(opts, args):
   
    input_file=None
    output_file=None    
 
    for opt, val in opts:
        if opt=='--help':
           print Usage
           sys.exit(1)
        if opt=='--input_file':
           input_file = val
        if opt=='--output_file':
           output_file =val
    
    if not input_file or not output_file: 
       print Usage
       sys.exit(1)


    cfg = ConfigFile( path = ('%s/json.template'%dir))
    j=cfg.get('JSON','json')   
    if input_file.startswith('~'): input_file= os.path.expanduser(input_file)
    if output_file.startswith('~'): output_file= os.path.expanduser(output_file)    
      
    
    ifile  = open(input_file, "rb")
    reader = csv.reader(ifile,delimiter='|')
    
    #ts = time.time()
    #open the file 
    f=open(output_file, 'w')
    for ar in reader:
       if len(ar) != 48: 
         print " Malformed line : %s " "".join(ar)
         continue
       getcontext().prec = eval(cfg.get('JSON','precision'))
       js=eval(j)
       json.dump(clean(js), f)
       f.write('\n')
    f.close()



if __name__ == '__main__':
    import sys
    import getopt
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["output_file=","input_file=", "help"])
    except getopt.GetoptError, msg:
        print 
        print msg
        print
        sys.exit(1)

    if  len(opts) <1 :
        print
        print Usage
        sys.exit(1)
        
    
    main( opts, args)
