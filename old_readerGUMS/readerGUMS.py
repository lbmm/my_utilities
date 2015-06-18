#!/bin/env python
from ConfigParser import ConfigParser
import string 
import os
import os.path
from datetime import datetime
from os import listdir
from os.path import isfile, join
from Queue import Queue
from threading import Thread


#used to resolve the configuration file outside the directory
dir=os.path.dirname(os.path.realpath(__file__))

Usage = """
readerGUMS.py 
Options:
    --input_dir     file that does contain list of lFNs
    --out_dir  output directory 
    --help     help                      
"""

class   ConfigFile(ConfigParser):
    def __init__(self,  path=None):
        ConfigParser.__init__(self)
        self.read(path)   


class AllowedValues:

   def __init__(self,cfg):
       self.fieldsNum=int(cfg.get('FIELD','tot_fields'))
       self.values={}
       
       for i in range(0,self.fieldsNum):
          self.values['values_%i'%i]=loadList(cfg.get('ALLOWED_VALUES','values_%i'%i)) 
      
       self.valuesSorted={}
       self.errCount=0
       self.errVerbose={}

   def incrementErr(self,value):
       self.errCount=self.errCount+1
       if not self.errVerbose.has_key(value):
          self.errVerbose[value]=0
       self.errVerbose[value]=self.errVerbose[value]+1


def loadData(allowedValues,cfg,lines):

    
    pos=int(cfg.get('FIELD','position'))

    #start to dig inside
    for line in lines:
       err=False
       arr=line.split("|")
       try:
         value=arr[pos-1]
       except IndexError:
          print line
          # count was not a number, so silently
          # ignore/discard this line
          continue
       
       tmp_key=''
       tmpValue=value
       to_continue=False
 
       for num in range(0,allowedValues.fieldsNum):
            
           key= next((x for x in allowedValues.values['values_%i'%num] if tmpValue.startswith(x)), None)
           if key: 
              tmp_key=tmp_key+key
              tmpValue=value[len(tmp_key):]
              
              #sorted all the fields
              if num is (allowedValues.fieldsNum-1):
                if not allowedValues.valuesSorted.has_key(tmp_key):
                       allowedValues.valuesSorted[tmp_key]=0
                allowedValues.valuesSorted[tmp_key]=allowedValues.valuesSorted[tmp_key]+1

           else:
              allowedValues.incrementErr(value)
              to_continue=True
              break 
       if to_continue:continue 
             
          
    return allowedValues

def printdata(allowedValues,dir_out,input_file):

    if '/' in input_file: 
       arr=input_file.split('/')
       file=''.join(arr[-1:])
        
    fd=open("%s/%s.out"%(dir_out,file),'w')
    for key in sorted(allowedValues.valuesSorted.iterkeys()):
        fd.write( "%s: %s \n" % (key, allowedValues.valuesSorted[key]))
    fd.close()

    fd=open("%s/%s.err"%(dir_out,file),'w')
    fd.write("Error count: %i \n" % allowedValues.errCount) 
   
    for key in sorted(allowedValues.errVerbose.iterkeys()):
       fd.write("%s: %s \n" % (key, allowedValues.errVerbose[key]))
    fd.close()

   

def loadList(lst):

  if lst!=None:
    # convert it to a list
    tmp = string.split(lst,",")
    lsts = []
    for el in tmp:
      sel = string.strip(el)
      if sel!="":
         lsts.append(sel)

  return lsts


def loadDict(lst):
    dict={}
    
    if lst!=None:
        dict   = eval(lst)

    return dict

def do_work(input_file,cfg,out_dir): 
    
    print "input_file %s" %input_file 
    now=datetime.now()
    time_connection=now.strftime("%H:%M:%S,%f")
    #print "tread reading %s @ %s" %(input_file,time_connection) 
    fs=open(input_file,"r")
    lines =fs.readlines()
    fs.close()
    now=datetime.now()
    time_connection=now.strftime("%H:%M:%S,%f")
    #print "tread end to read %s @ %s" %(input_file,time_connection)

    aV = AllowedValues(cfg)
    loadData(aV,cfg,lines)
    printdata(aV,out_dir,input_file)
 

def worker(q,cfg,out_dir):
  while True:
    input_file = q.get()
    do_work(input_file,cfg,out_dir)
    q.task_done()


def main(opts, args):
   
    input_dir=None
    out_dir=None    
 
    for opt, val in opts:
        if opt=='--help':
           print Usage
           sys.exit(1)
        if opt=='--input_dir':
           input_dir = val
        if opt=='--out_dir':
           out_dir =val
    
    if not input_dir or not out_dir: 
       print Usage
       sys.exit(1)

    if not os.path.exists(out_dir):
       print "%s doesn't exist" %out_dir
       sys.exit(1)

    cfg = ConfigFile( path = ('%s/reader.cfg'%dir))
    #cfg = ConfigFile( path = ('%s/reader_variability.cfg'%dir))
   
    if input_dir.startswith('~'): input_dir= os.path.expanduser(input_dir)
    if out_dir.startswith('~'): out_dir= os.path.expanduser(out_dir)    
   
    num_worker_threads=int(cfg.get('THREADS','num_worker'))


    source_files = [ ("%s/%s")%(input_dir,f) for f in listdir(input_dir) if isfile(join(input_dir,f)) ]

    q = Queue()
    for i in range(num_worker_threads):
        #print "spanning thread %i" %i
        t = Thread(target=worker,args=(q,cfg,out_dir))
        t.daemon = True
        t.start()

    for item in source_files:
        q.put(item)

    q.join()       # block until all tasks are done

 
 


if __name__ == '__main__':
    import sys
    import getopt
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["out_dir=","input_dir=", "help"])
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
