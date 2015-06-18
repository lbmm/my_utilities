#!/bin/env python
import string 
import os
import os.path
from os import listdir
from os.path import isfile, join


#used to resolve the configuration file outside the directory
dir=os.path.dirname(os.path.realpath(__file__))

Usage = """
concatenate_files.py
Options:
    --input_dir            dir that contains files to concatenat (mandatory) 
    --n_final_files        number of final files            (mandatory)
    --help       help                      
"""




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




def main(opts, args):
   
    input_dir,n_final_files=None,0
 
    for opt, val in opts:
        if opt=='--help':
           print Usage
           sys.exit(1)
        if opt=='--input_dir':
           input_dir = val
        if opt=='--n_final_files':
           n_final_files =int(val)
    
    if not input_dir or not n_final_files: 
       print Usage
       sys.exit(1)



    #expand the path to a absolute path
    if input_dir.startswith('~'): input_file= os.path.expanduser(input_file)
  
    files_to_reduce = [ ("%s/%s")%(input_dir,f) for f in listdir(input_dir) if isfile(join(input_dir,f)) ]

    step=len(files_to_reduce)/n_final_files 
   
    for i in range(0,len(files_to_reduce),step):
       file_target=files_to_reduce[i]
       try:
           files_source=files_to_reduce[i+1:i+step]
       except  IndexError:
           files_source=files_to_reduce[i:+1]
       print 'cat %s >> %s  \n'%(' '.join(files_source),file_target)
       print 'rm %s \n' % ' '.join(files_source) 
       
                  
 



if __name__ == '__main__':
    import sys
    import getopt
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["input_dir=","n_final_files=", "help"])
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
