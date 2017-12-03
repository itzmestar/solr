import os, threading, json, sys, random
import requests
import time, platform, logging, signal

#########Logging configuration##########
if platform.platform().startswith('Windows'):
	logging_file = os.path.join(os.getcwd(),'solr_post.log')
else:
	logging_file = os.path.join(os.getcwd(), 'solr_post.log')
    
logging.basicConfig(
        level=logging.INFO,
		#level=logging.DEBUG,
        format='%(asctime)s : %(levelname)s : %(threadName)-9s : %(message)s',
        filename=logging_file,
        filemode='w',
)
log_update_count=100 #every this count update the logs for statictics
#########configuration##########
thread_num=5
time_to_run=20  #in seconds, -1 for infinite

solr="192.168.43.92"
port=8983
collection="finance"

user='xyz'
passwd='xyz123'
#----query string related-----#
col="Date"
start_year=1996
end_year=2017

#-----post related-----#
headers={'Content-type':'application/csv'}
timeout=5

#-----global variables-----#
post_threads = []
file_list = []

#-----Classes & functions-----#

class Post_maker(threading.Thread):
	total_post = 0
	average_response_time = 0
	lock = threading.Lock()
    
	def __init__(self, index):
		threading.Thread.__init__(self)
		self.shutdown_flag = threading.Event()
		self.__index = index
		self.url = get_post_url()
		self.__successful_post = -1
		self.__post_count = 0
		self.__total_response_time = 0
		self.__avg_response_time = 0

	def incr_post_count(self):
		self.__post_count += 1

	def incr_response_time(self,span):
		self.__total_response_time += span
        
	def run(self):
        #run till shutdown_flag is not set
		while not self.shutdown_flag.is_set():
			t1=time.time()
			filename=pick_rand_file(file_list)
			logging.debug ('filename = %s',filename)
			payload=read_file(filename)
			response = requests.post(self.url, auth=(user, passwd), headers=headers, data=payload, timeout=timeout)
			t2=time.time()
			self.incr_post_count()
			logging.debug ('%s', response.json())
			self.incr_response_time(t2-t1)
			logging.debug ('response code: %d ',response.status_code)
			#update counters every log_update_count posts
			if self.__post_count % log_update_count == 0:
				self.update_counters()
		self.update_counters()

	def update_counters(self):
		self.__avg_response_time = find_average(self.__total_response_time,self.__post_count)
		logging.debug ("Number of posts: %d", self.__post_count)
		logging.debug ("Total response time for posts: %0.02f", self.__total_response_time)
		logging.debug ('Average response time for posts: %0.02f',self.__avg_response_time)
		Post_maker.lock.acquire()
		try:
            #finish the work: take a lock & update the class variables
			Post_maker.total_post += self.__post_count
			Post_maker.average_response_time += self.__avg_response_time
		finally:
			Post_maker.lock.release()
        
	@classmethod 
	def print_final_statistics(cls):
		logging.info ("=========================================================")
		logging.info ("Total number of threads created for posts: %d", thread_num)
		logging.info ("Total number of posts run: %d", cls.total_post)
		logging.info ("Average response time for posts: %0.02f", find_average(cls.average_response_time,thread_num))
		logging.info ("=========================================================")

def find_average(num, total):
	if total >0:
		return (num/total)
	else:
		return (0)
        
def usage():
	print ('Usage : python3 solr_post.py <num of threads> <time to run in seconds>')

def get_query(start_val, end_val):
	n = random.randint(start_val,end_val)
	query_str = "%s:%d*" % (col, n)
	return (query_str)
   
def get_query_url():
	temp = "http://%s:%d/solr/%s/select?q=" % (solr, port, collection)
    
	url = "%s%s&wt=json" % (temp, get_query(start_year, end_year))
	logging.debug ('%s', url)
	return (url)

def read_file(filename):
	with open(filename, 'rb') as f:
		payload = f.read()
	return (payload)

def pick_rand_file(list):
	n = random.randint(0,len(list)-1)
	return (list[n])

def populate_file_list():
	global file_list 
	for file in os.listdir():
		if os.path.isfile(file) and file.endswith(".csv"):
			file_list.append(file)
			
	
''' 
#sample post curl command:

curl 'http://localhost:8983/solr/my_collection/update?commit=true' --data-binary
@example/exampledocs/books.csv -H 'Content-type:application/csv'

'''
def get_post_url():
	url = "http://%s:%d/solr/%s/update?commit=true" % (solr, port, collection)
	logging.debug ('%s', url)
	return (url)

def cancel_threads():
	global post_threads
	logging.debug ('Times up! Shutting down threads...')
	for th in post_threads:
		th.shutdown_flag.set()
	return

def finish():
	for el in post_threads:
		el.join()

	Post_maker.print_final_statistics()
    
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

#populate file list
populate_file_list()

if len(file_list) == 0:
	print("No file found with .csv extention in current dir. Exiting")
	logging.info('No file found with .csv extention in current dir. Exiting')
	sys.exit()

# Register the signal handlers
signal.signal(signal.SIGTERM, service_shutdown)
signal.signal(signal.SIGINT, service_shutdown)
    
#create n threads
for i in range(thread_num):
	post=Post_maker(i)
	post_threads.append(post)
	post.start()

#start a timer thread
if time_to_run > -1:
	timer = threading.Timer(time_to_run, cancel_threads)
	timer.setName('Timer')
	timer.start()
    
finish()
