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

output=[]

class   ConfigFile(ConfigParser):
    def __init__(self,  path=None):
        ConfigParser.__init__(self)
        self.read(path)   

             
   

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

def do_work(host,database): 
    
    now=datetime.now()
    # here the DB
    cmd = "time mysql -u h %s --database %s < "% (host,database) 
    stdin, stdout, stderr = os.popen3(cmd)
    stdin.close()
    errmsg = stderr.readlines()
    outmsg = stdout.readlines()
    stdout.close()
    stderr.close()
    print "host : %s database:%s" %(host,database)
    print errmsg
    print outmsg
    #end DB connection

 

def worker(q,database):
  while True:
    host = q.get()
    do_work(host,database)
    q.task_done()


def main(opts, args):

    cfg = ConfigFile( path = ('%s/bladeMysql.cfg'%dir))
   
    hosts=loadList(cfg.get('DB','hosts'))
    dbname=None 
     

    num_worker_threads=int(cfg.get('TH','numbers'))

    q = Queue()
    for i in range(num_worker_threads):
        #print "spanning thread %i" %i
        t = Thread(target=worker,args=(q))
        t.daemon = True
        t.start()

    for host  in hosts:
        q.put(host)

    q.join()       # block until all tasks are done
 


if __name__ == '__main__':
    
        
    
    main( )
