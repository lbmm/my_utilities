import threading
import sys
import MySQLdb
import pdb

class threader(threading.Thread):
    def __init__(self,db):
        threading.Thread.__init__(self)
        self.db = db
    def run(self):
        run_insert(self.db)

def run_insert(db):
    sql = "INSERT INTO `test` (`id`,`col_a`,`col_b`,`col_c`) VALUES (NULL,'0','0','0');"
    print "thread executing sql:%s"%(sql)
    try:
        cursor = db.cursor()
        cursor.execute(sql)
        cursor.close()
        db.commit()
    except:
        print "insert failed"

    return

def init_thread(): 
    backgrounds = []
    for db in connections:
        print "connection: %s"%(db)
        background = threader(db)
        background.start()
        backgrounds.append(background)

    for background in backgrounds:
        background.join()

def main():
    try:
        init_thread()
    except:
        print "failed to initiate threads"
    sys.exit(0)

if __name__ == "__main__":
    mysql_host = "localhost" 
    mysql_user = "root" 
    mysql_pass = "password" 
    mysql_port = int(3306)
    mysql_db = "test" 
    threads = int(4) #quantity of execution threads and size of connection pool

    connections = []
    for thread in range(threads):
        try:
            connections.append(MySQLdb.connect(host=mysql_host, user=mysql_user, passwd=mysql_pass, db=mysql_db, port=mysql_port))
            #pdb.set_trace()

        except MySQLdb.Error, e:
            print "Error %d: %s"%(e.args[0], e.args[1])
            pdb.set_trace()            
            sys.exit (1)

    main()
    
