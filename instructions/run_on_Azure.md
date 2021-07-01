# 1) 
**to deploy a kafka - spark structured streaming cluser on Azure, do the following: Use Apache Spark Structured Streaming with Apache Kafka on HDInsight** 
[SparkStruturedStreaming-kafka-Azure](https://docs.microsoft.com/it-it/azure/hdinsight/hdinsight-apache-kafka-spark-structured-streaming)

# 2) install jq on your local machine
```
sudo apt update
sudo apt install jq
```

# 3) Deploy the Spark-kafka cluster using a template that is available in Microsoft website:
- [template deploy azure kafka-spark cluster](https://docs.microsoft.com/it-it/azure/hdinsight/hdinsight-apache-kafka-spark-structured-streaming)


Cluster size - Spark
Node type|Node size|Number of nodes
---------|---------|---------------
Head|D12 v2 (4 Cores, 28 GB RAM)|2
Worker|D13 v2 (8 Cores, 56 GB RAM)|4
Zookeeper|A2 v2 (2 Cores, 4 GB RAM)|3

Cluster size - Kafka
Node type|Node size|Number of nodes
---------|---------|---------------
Head|D12 v2 (4 Cores, 28 GB RAM)|2
Worker|D13 v2 (8 Cores, 56 GB RAM)|4
Zookeeper|A2 v2 (2 Cores, 4 GB RAM)|3


- *check that you have resources*
- homepage - subscriptions - subscription title (microsoft azure sponsorship 2) - Usage + quotas - youo need more than 30 vCPUs in Total Regional vCPUs

# 4) start using the cluster

1. Gather host information

```
export password='YOUR_KAFKA_CLUSTER_PASSWORD'
export CLUSTERNAME=YOUR_KAFKA_CLUSTER_NAME

```
```

curl -sS -u admin:$password -G "https://YOUR_KAFKA_CLUSTER_NAME.azurehdinsight.net/api/v1/clusters/skafka/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2
```

- Replace `YOUR_KAFKA_CLUSTER_NAME` with the name of your Kafka cluster, and `YOUR_KAFKA_CLUSTER_PASSWORD` with the cluster login password.
2. From a web browser, navigate to https://CLUSTERNAME.azurehdinsight.net/jupyter, where CLUSTERNAME is the name of your `Spark` cluster. When prompted, enter the cluster login (`admin` is the default) and `Spark` cluster password used when you created the cluster.
4. Use the curl and jq commands  to obtain broker hosts information.

```
curl -sS -u admin:$password -G https://YOUR_KAFKA_CLUSTER_NAME.azurehdinsight.net/api/v1/clusters/skafka/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2
```
# 5) Copying required jars!
**this project depends on some spatial processing libraries above Apache Spark, you need to load them to the project in order to be able to call them in Jupyter**
- [find the jars here](./jars/)
- copy the file titled `magellan-1.0.5-s_2.11.jar` to the `storage account of your Spark cluster`
- `this .jar library will be used as a dependency that will be added to the library using the Jupyter Spark magic command`
- specifically, it will be imported in the first cell
```
%%configure -f
{
    "conf": {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0",
        "spark.jars":"wasbs://sspark@7fek46h7orhig.blob.core.windows.net/jars/magellan-1.0.5-s_2.11.jar",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11",
        "spark.dynamicAllocation.enabled": false
    }
}
```
# 6) running java kafka producer and send data to Azure Kafka cluster

***copying files***

- copy the Kafka Java producer fat .jar file `saosKafkaProducer-1.0-SNAPSHOT.jar` [find it here](./jars)  from this folder  to a folder in your kafka cluster in Azure
```
scp ./target/saosKafkaProducer-1.0-SNAPSHOT.jar USER_NAME@YOUR_KAFKA_CLUSTER_NAME-ssh.azurehdinsight.net:kafka-producer-consumer.jar
```
> replace `USER_NAME` with the user name you have chosen when you created the cluster `sshuser` is the default!
- copy the geojson file **shenzhen_converted.geojson** to the same working directory
```
scp guang.csv USER_NAME@YOUR_KAFKA_CLUSTER_NAME-ssh.azurehdinsight.net:guang.csv
```


```
in order to be able to access the Neigboors .geojson file, 
we need to store it in a blob storage:
- go to "HDInsight clusters" --> Spark cluster name --> search for "storage accounts",
- select the "Azure Storage" name 
- storage explorer --> blob containers --> sspark --> create new folder "datasets"
- upload shenzhen_converted.geojson
```
[find it here](../data/)
```
- then you can access it in your notebook using:
- "wasb://CONTAINER_NAME@STORAGE_ACCOUNT_NAME.blob.core.windows.net/datasets/shenzhen_converted.geojson"
- where sspark is the spark cluster name
```
> replace `CONTAINER_NAME` with the container name in your Spark storage account where you hosted the `shenzhen_converted.geojson` regions file. ALSO, replace `STORAGE_ACCOUNT_NAME` with the name of your Spark `storage account`

- copy the `Electric Vehicle Data` mobility data [find it here](https://www.cs.rutgers.edu/~dz220/Data.html)
- "...N.B. if you are using this data **add the following reference** ..." [[1]](#1).

```
scp guang.csv USER_NAME@YOUR_KAFKA_CLUSTER_NAME-ssh.azurehdinsight.net:guang.csv 
```

# 7) to run kafka producer
1. create the topic in Jupyter
2. login to the headnode of kafka cluster
  - navigate to kafka cluster 'skafka' | SSH + Cluster login
  - copy the login command and use it in your local machine's terminal
   > ssh USER_NAME@YOUR_KAFKA_CLUSTER_NAME-ssh.azurehdinsight.net
2. get the kafkaBrokers list running the following command in your local machine

```
sudo apt -y install jq
export password='cluster_pass'

export KAFKABROKERS=$(curl -sS -u admin:$password -G https://YOUR_KAFKA_CLUSTER_NAME.azurehdinsight.net/api/v1/clusters/skafka/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2);
```


3. run the following command to *start the kafka producer* in **kafka cluster head node**

```
**you need to create the topic first, maybe in the jupyter notebook with the %%bash magic command**
```

```
java -jar kafka-producer-consumer.jar shenzhen spatial1 $KAFKABROKERS /home/isam/guang.csv 1
```
 - kafka java producer takes the following parameters
 ```
  args[0] -->  data     :(String) 
  args[1] -->  topicName :(type:string)
  args[2] -->  brokers :(String)
  args[3] -->  path    :(String) 
  args[4] -->  time    : int  
        
        time is the time between tuples generated
        to get the path:
        pwd in the kafka cluster headnode
        data is either shenzhen or nyc
 ```
# 8) run the Jupyter notebook [find it here](../notebooks)
# N.B. if you are using this code, please cite our works
"...first ..." [[2]](#2).
"...second ..." [[3]](#3).
"...third ..." [[4]](#4).

## References
- <a id="1">[1]</a> 
Wang, Guang, et al. (2019). 
"Experience: Understanding long-term evolving patterns of shared electric vehicle networks.". 
The 25th Annual International Conference on Mobile Computing and Networking. 2019.
- <a id="2">[2]</a> 
Al Jawarneh, Isam M., Paolo Bellavista, Antonio Corradi, Luca Foschini, and Rebecca Montanari.  (2021)
"QoS-Aware Approximate Query Processing for Smart Cities Spatial Data Streams".
Sensors 21, no. 12: 4160. https://doi.org/10.3390/s21124160
