import os, threading, json, sys, random
from urllib.request import urlopen
import time, platform, logging, signal

#########Logging configuration##########
if platform.platform().startswith('Windows'):
    logging_file = os.path.join(os.getcwd(),'solr_query.log')
else:
    logging_file = os.path.join(os.getcwd(), 'solr_query.log')
    
logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s : %(levelname)s : %(threadName)-9s : %(message)s',
        filename=logging_file,
        filemode='w',
)

#########configuration##########
thread_num=10
time_to_run=20  #in seconds, -1 for infinite

solr="192.168.43.92"
port=8983
collection="finance"

#----query string related-----#
col="Date"
start_year=1996
end_year=2017

#-----global variables-----#
query_threads = []

#-----Classes & functions-----#

class Query_maker(threading.Thread):
    total_query = 0
    average_response_time = 0
    lock = threading.Lock()
    
    def __init__(self):
        threading.Thread.__init__(self)
        self.shutdown_flag = threading.Event()
        self.url = get_query_url()
        self.__successful_query = -1
        self.__query_count = 0
        self.__total_response_time = 0
        self.__avg_response_time = 0

    def incr_query_count(self):
        self.__query_count += 1

    def get_query_count(self):
        return (self.__query_count)

    def incr_response_time(self,span):
        self.__total_response_time += span
        
    def get_response_time(self):
        return (self.__total_response_time)
        
    def run(self):
        #run till shutdown_flag is not set
        while not self.shutdown_flag.is_set():
            self.incr_query_count()
            t1=time.time()
            response = urlopen(self.url)
            html = response.read()
            t2=time.time()
            jresp = json.loads(html.decode('utf-8'))
            logging.debug ('%s', jresp)
            self.incr_response_time(t2-t1)
            logging.info ('%d documents found.',jresp['response']['numFound'])
            self.url = get_query_url()
        self.update_counters()

    def update_counters(self):
        self.__avg_response_time = find_average(self.get_response_time(),self.get_query_count())
        logging.info ("Number of queries run: %d", self.get_query_count())
        logging.info ("Total response time for queries: %0.02f", self.get_response_time())
        logging.info ('Average response time for queries: %0.02f',self.__avg_response_time)
        Query_maker.lock.acquire()
        try:
            #finish the work: take a lock & update the class variables
            Query_maker.total_query += self.get_query_count()
            Query_maker.average_response_time += self.__avg_response_time
        finally:
            Query_maker.lock.release()
        
    @classmethod 
    def print_statistics(cls):
        logging.info ("=========================================================")
        logging.info ("Total number of threads created for queries: %d", thread_num)
        logging.info ("Total number of queries run: %d", cls.total_query)
        logging.info ("Average response time for queries: %0.02f", find_average(cls.average_response_time,thread_num))
        logging.info ("=========================================================")

def find_average(num, total):
    if total >0:
        return (num/total)
    else:
        return (0)
        
def usage():
    print ('Usage : python3 solr_query.py <num of threads> <time to run in seconds>')

def get_query(start_val, end_val):
    n = random.randint(start_val,end_val)
    query_str = "%s:%d*" % (col, n)
    return (query_str)
    
def get_query_url():

    temp = "http://%s:%d/solr/%s/select?q=" % (solr, port, collection)
    
    url = "%s%s&wt=json" % (temp, get_query(start_year, end_year))
    logging.debug ('%s', url)
    return (url)

def cancel_threads():
    global query_threads
    logging.debug ('Times up! Shutting down threads...')
    for th in query_threads:
        th.shutdown_flag.set()
    return

def finish():
    for el in query_threads:
        el.join()

    Query_maker.print_statistics()
    
def service_shutdown(signum, frame):
    logging.info('Caught signal %d' % signum)
    cancel_threads()
    finish()

########## Main Program starts here ########

if len(sys.argv) == 3 :
    thread_num = int(sys.argv[1])
	time_to_run = int(sys.argv[2])
	
elif len(sys.argv) > 1:
	usage()
	sys.exit()



''' #Sample query string
#url="http://192.168.43.92:8983/solr/finance/select?q=Date:1999-01-*&wt=json"
'''

# Register the signal handlers
signal.signal(signal.SIGTERM, service_shutdown)
signal.signal(signal.SIGINT, service_shutdown)
    
#create n threads
for i in range(thread_num):
    query=Query_maker()
    query_threads.append(query)
    query.start()

#start a timer thread
if time_to_run > -1:
    timer = threading.Timer(time_to_run, cancel_threads)
    timer.setName('Timer')
    timer.start()
    
finish()
