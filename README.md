# Python3 script to test Apache Solr Performance

It uses number of threads (configurable) to query Solr for a finite/infinite number of seconds & print the average query time to log file along with total num of queries, total time for response etc.

It performs the following query:
http://solr-hostname:8983/solr/finance/select?q=Date:year*&wt=json
  where year is randomly generated for each query between a given range.
  
In case, script is run with infinite num of seconds, it can be stopped with Ctrl + c signal.

How to run:
#python3 solr_query.py
 OR
#python3 solr_query.py 5 10

keywords: 
# python3 class signal lock logging threading timer Solr 
