# Python3 script to perform load test on Apache Solr measuring its Performance

# solr_query.py
It uses number of threads (configurable) to query Solr for a finite/infinite number of seconds & print the average query time to log file along with total num of queries, total time for response etc.

It performs the following query:
http://solr-hostname:8983/solr/finance/select?q=Date:year*&wt=json
  where year is randomly generated for each query between a given range.
  
In case, script is run with infinite num of seconds, it can be stopped with Ctrl + c signal.

How to run:
$ python3 solr_query.py
 OR
$ python3 solr_query.py 5 10


# solr_post.py

This script posts .csv file format documents to solr for indexing. Script can be used for load testing of solr as it can be run for specified number of seconds with specified number of threads.
Place the script in the same directory as the *.csv files & run it.

How to run:
$ python3 solr_post.py <num of threads> <time to run in seconds>
arg1 & arg2 are optional.
  
keywords: 
# python3 class signal lock logging threading timer Solr query post


