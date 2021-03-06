# Managing-data-streams-using-Kafka-Zookeeper-Spark-Event-Message-Queues

## _Optimally manage data pipelines with reliability and durability with brokers_

<a href="https://www.linkedin.com/in/alandanque"> Author: Alan Danque </a>

<a href="https://adanque.github.io/">Click here to go back to Portfolio Website </a>

![A remote image](https://adanque.github.io/assets/img/Spark.jpg)

Event queue message publication to subscriber via Kafka and Spark streams over Zookeeper, Hadoop.

## Pythonic Libraries Used in this project
- uuid
- json
- kafka
- pyspark
- pandas
- threading
- decimal
- shutil
- pyarrow
- glob
- dask
- pathlib
- time
- os

## Repo Folder Structure

├───checkpoints

│   ├───accelerations

│   ├───locations

│   └───locations-windowed

└───results

    ├───input
	
    │   ├───accelerations
	
    │   └───locations
	
    ├───model_1
	
    └───output
	
        └───simple

## Python Files 

| File Name  | Description |
| ------ | ------ |
| run_Kafka_Topic_Create.py | Creates kafka topic for readstream and writestream|
| run_Spark_Dataframe_Update.py | Spark dataframe update |
| run_Join_Spark_Stream_Dataframe.py | Joining spark streaming dataframes |
