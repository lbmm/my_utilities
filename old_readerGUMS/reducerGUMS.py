#!/bin/env python
import string 
import os
import os.path
from os import listdir
from os.path import isfile, join


#used to resolve the configuration file outside the directory
dir=os.path.dirname(os.path.realpath(__file__))

Usage = """
reducerGUMS.py 
Options:
    --input_dir  dir that contains files to reduce (mandatory) 
    --ext        extension of the files            (mandatory)
    --help       help                      
"""



final_values={}

def printdata(ext):

    fname="reducer"    
    fd=open("%s.%s"%(fname,ext),'w')
    tot_counted=0
    for key in sorted(final_values.iterkeys()):
        fd.write( "%s: %s \n" % (key, final_values[key]))
        tot_counted=tot_counted+final_values[key]

    if not ext.startswith("err"):
       fd.write("Total counted: %s \n"%tot_counted)
    fd.close()


def loadData(input_file):


    fs=open(input_file,"r")
    lines =fs.readlines()
    fs.close()

    for line in lines:
 
       key,value=line.split(":")
      
       if not final_values.has_key(key):
          final_values[key]=0
       final_values[key]=final_values[key]+int(value)


def main(opts, args):
   
    input_dir,ext=None,None
 
    for opt, val in opts:
        if opt=='--help':
           print Usage
           sys.exit(1)
        if opt=='--input_dir':
           input_dir = val
        if opt=='--ext':
           ext =val
    
    if not input_dir or not ext: 
       print Usage
       sys.exit(1)



    #expand the path to a absolute path
    if input_dir.startswith('~'): input_file= os.path.expanduser(input_file)
  
    files_to_reduce = [ ("%s/%s")%(input_dir,f) for f in listdir(input_dir) if isfile(join(input_dir,f)) and f.endswith(ext) ] 
   
    
    for file_to_reduce in files_to_reduce:
        loadData(file_to_reduce)

 
    printdata(ext)



if __name__ == '__main__':
    import sys
    import getopt
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["input_dir=","ext=", "help"])
    except getopt.GetoptError, msg:
        print 
        print msg
        print
        print Usage
        sys.exit(1)

    if  len(opts) <1 :
        print
        print Usage
        sys.exit(1)
        
    
    main( opts, args)
