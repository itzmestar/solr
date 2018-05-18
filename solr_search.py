import os, threading, json, sys
import requests
import time, platform, logging
import argparse

#########Logging configuration##########
if platform.platform().startswith('Windows'):
    logging_file = os.path.join(os.getcwd(),'solr_search.log')
else:
    logging_file = os.path.join(os.getcwd(), 'solr_search.log')
    
logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s : %(levelname)s : %(threadName)-9s : %(message)s',
        filename=logging_file,
        filemode='w',
)

#########configuration##########
thread_num=6

solr="localhost"

port=8983

cores_list=["techproducts","techproducts","techproducts","techproducts","techproducts","techproducts"]

#----query string related-----#
column="username"
search_string=""
rows=10

#resp_format="xml"
resp_format="json"
#-----global variables-----#
query_threads = []

#-----Classes & functions-----#

class Query_maker(threading.Thread):

    lock = threading.Lock()
    outfile="response.txt"
       
    def __init__(self, core_id):
        threading.Thread.__init__(self)
        self.url = ""
        self.core = cores_list[core_id]
        try:
            os.remove(Query_maker.outfile)
        except OSError:
            pass
        
        
    def run(self):
        self.url = get_query_url(self.core, column, search_string)
        t1=time.time()
        response = requests.get(self.url)
        t2=time.time()
        response_time=t2-t1
        if resp_format == "json":
            jresp = response.json()
            #print(jresp)
            #logging.debug ('%s', jresp)
            self.dump_response_json(jresp)
        elif resp_format == "xml":
            #print (response.content)
            #print (response)
            self.dump_response_xml(response)
        elif resp_format == "python":
            #print (response.content)
            #print (response)
            #self.dump_response_py(response)
            pass
        logging.info ('Received a response in time: %d', response_time)
        
        #logging.debug ('%d search results found in time: %d.',jresp['response']['numFound'], response_time)

    def dump_response_xml(self, response):
        Query_maker.lock.acquire()
        try:
            msg="\nResponse from core: {}\n".format(self.core)
            with open(Query_maker.outfile, 'ab') as file:
                file.write(msg.encode('ascii'))
                file.write(response.content)
                    
            file.close()
            fileContents = open(Query_maker.outfile,"r").read()
            f = open(Query_maker.outfile,"w", newline="\r\n")
            f.write(fileContents)
            f.close()
        finally:
            Query_maker.lock.release()

    def dump_response_json(self, response):
        Query_maker.lock.acquire()
        try:
            msg="\nResponse from core: {}\n".format(self.core)
            with open(Query_maker.outfile, 'ab') as file:
                file.write(msg.encode('ascii'))
                msg="Number of results found: {}\n".format(response['response']['numFound'])
                file.write(msg.encode('ascii'))
                for doc in response['response']['docs']:
                    for key, value in doc.items():
                        msg='{} : {}\n'.format(key, value)

                        file.write(msg.encode('utf-8'))

                    #file.write(response.content)
            file.close()
        finally:
            Query_maker.lock.release()
            
def get_query(col, search_str):
    query_str = "%s:%s" % (col, search_str)
    return (query_str)
    
def get_query_url(core, col, search_str):

    temp = "http://%s:%d/solr/%s/select?q=" % (solr, port, core)
    
    url = "%s%s&rows=%s&wt=%s&indent=true" % (temp, get_query(col, search_str), rows, resp_format)
    logging.debug ('%s', url)
    return (url)

def finish():
    for el in query_threads:
        el.join()

########## Main Program starts here ########

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search Solr.')
    parser.add_argument('--col', help='column name', type=str, required=True)
    parser.add_argument('--str', help='search string', type=str, required=True)
    parser.add_argument('--row', type=int, default=10)
    args = parser.parse_args()

    column=args.col
    search_string=args.str
    rows=args.row

    ''' #Sample query string
    #url="http://192.168.1.105:8983/solr/techproducts/select?q=id:*&wt=xml"
    '''
    
    #create n threads
    for i in range(thread_num):
        query=Query_maker(i)
        query_threads.append(query)
        query.start()
        
    finish()
