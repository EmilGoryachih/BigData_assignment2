## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the full local text corpus used for the final run.

### data_100
This folder stores the fixed 100-document debug subset used for quick iteration and screenshots.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.

### app.py
This Python CLI creates the Cassandra schema and loads index data from HDFS into Cassandra.

### app.sh
The main entrypoint for the repository. It starts Hadoop services, prepares data, builds the index, loads it into Cassandra, and runs sample searches. The default mode is the full corpus in `data`, and `DATASET_MODE=debug` switches to `data_100`.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS.

### index.sh
A script to run the MapReduce pipelines and the programs to store data in Cassandra/ScyllaDB.

### prepare_data.py
The script that reads plain text documents from HDFS with PySpark RDD operations and builds the prepared one-partition indexing input.

### prepare_data.sh
The script that uploads a chosen local corpus folder to HDFS and runs `prepare_data.py`.

### query.py
A PySpark BM25 ranker that reads the query, fetches postings and statistics from Cassandra, computes scores with the RDD API, and prints the top 10 documents.

### requirements.txt
This file contains all Python depenedencies that are needed for running the programs in this repository. This file is read by pip when installing the dependencies in `app.sh` script.

### search.sh
This script runs `query.py` on Hadoop YARN with the packaged Python environment.


### start-services.sh
This script will initiate the services required to run Hadoop components. This script is called in `app.sh` file.


### store_index.sh
This script will create Cassandra/ScyllaDB tables and load the index data from HDFS to them.
