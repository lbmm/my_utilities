import csv
import queries as q
from ConfigParser import ConfigParser
import MySQLdb

Usage = """
exportToCsv.py
very simple piece of code, that from a 
MySQL query output into a csv file
Options:
    --output_file  output csv file
"""


class   ConfigFile(ConfigParser):
    def __init__(self,  path=None):
        ConfigParser.__init__(self)
        self.read(path)

class DBConnection:
    def __init__(self,cfg):
        self.User = cfg.get('DB','user')
        self.Password = cfg.get('DB','password')
        self.Host = cfg.get('DB','host')
        self.DBName = None


        self.DBName=cfg.get('DB','dBName')

        try:
          self.DB = MySQLdb.connect(host=self.Host,user=self.User,passwd=self.Password,db= self.DBName)
        except MySQLdb.Error, e:
               print "Connection Error %d: %s:"(e.args[0], e.args[1])

    def cursor(self):
       return self.DB.cursor()



def main(opts, args):

    output_file=None
    for opt, val in opts:
     if opt=='--help':
        print Usage
        sys.exit(1)
     if opt=='--output_file':
        output_file= val


     if not output_file:
       print "please provide an output_file"
       sys.exit(1)

     cfg = ConfigFile( path = 'db.cfg')
     db =DBConnection(cfg)
     cursor=db.cursor()
     cursor.execute(q.info)
     data=cursor.fetchall()

     with open(output_file, 'wb') as fout:
         writer = csv.writer(fout)
         writer.writerow([ i[0] for i in cursor.description ]) # heading row
         for d in data:
            writer.writerow(d)

     fout.close()

if __name__ == '__main__':
   import sys
   import getopt
    
   try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["output_file="])
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
