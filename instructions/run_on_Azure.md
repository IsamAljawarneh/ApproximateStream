# first
**to deploy a kafka - spark structured streaming cluser on Azure, do the following: Use Apache Spark Structured Streaming with Apache Kafka on HDInsight** 
[SparkStruturedStreaming-kafka-Azure](https://docs.microsoft.com/it-it/azure/hdinsight/hdinsight-apache-kafka-spark-structured-streaming)

# install jq on your local machine
```
sudo apt update
sudo apt install jq
```

# Deploy the Spark-kafka cluster using a template that is available in Microsoft website:
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

# start using the cluster

1. Gather host information

- > curl -sS -u admin:$password -G "https://%CLUSTERNAME%.azurehdinsight.net/api/v1/clusters/%CLUSTERNAME%/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2

```
export password='cluster_pass'
export CLUSTERNAME=skafka

```
```

curl -sS -u admin:$password -G "https://skafka.azurehdinsight.net/api/v1/clusters/skafka/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2
```

- Replace KafkaCluster with the name of your Kafka cluster (skafka), and KafkaPassword with the cluster login password.
2. From a web browser, navigate to https://CLUSTERNAME.azurehdinsight.net/jupyter, where CLUSTERNAME is the name of your spark cluster (sspark in this case). When prompted, enter the cluster login (admin) and password used when you created the cluster.
4. Use the curl and jq commands  to obtain broker hosts information.
- > curl -sS -u admin:$password -G https://skafka.azurehdinsight.net/api/v1/clusters/skafka/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2

```
curl -sS -u admin:$password -G https://skafka.azurehdinsight.net/api/v1/clusters/skafka/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2
```
# Copying required jars!
**this project depends on some spatial processing libraries above Apache Spark, you need to load them to the project in order to be able to call them in Jupyter**
- [find the jars here](./instructions/jars/)
# running java kafka producer and send data to Azure Kafka cluster

- **copying files**

- copy the .jar fat file from the 'target' folder to a folder in the kafka cluster in Azure
- > scp ./target/saosKafkaProducer-1.0-SNAPSHOT.jar isam@skafka-ssh.azurehdinsight.net:kafka-producer-consumer.jar
- copy the geojson file **shenzhen_converted.geojson** to the same working directory
- > scp guang.csv isam@skafka-ssh.azurehdinsight.net:guang.csv

```
 scp nyc.csv isam@skafka-ssh.azurehdinsight.net:nyc.csv 
 ```


```
in order to be able to access the Neigboors .geojson file, 
we need to store it in a blob storage:
- go to "HDInsight clusters" --> Spark cluster name --> search for "storage accounts",
- select the "Azure Storage" name 
- storage explorer --> blob containers --> sspark --> create new folder "datasets"
- upload shenzhen.geojson
- then you can access it using
- "wasb://sspark@7q6kgdctotuwu.blob.core.windows.net/datasets/shenzhen_converted.geojson"
- where sspark is the spark cluster name
```
# to run kafka producer
1. create the topic in Jupyter
2. login to the headnode of kafka cluster
  - navigate to kafka cluster 'skafka' | SSH + Cluster login
  - copy the login command and use it in your local machine's terminal
   > ssh isam@skafka-ssh.azurehdinsight.net
2. get the kafkaBrokers list running the following command in your local machine
```
curl -sS -u admin:$password -G https://skafka.azurehdinsight.net/api/v1/clusters/skafka/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2
```
 - *OR* in kafka cluster head node **(install jq also in the headnode)**
```
sudo apt -y install jq
export password='cluster_pass'

export KAFKABROKERS=$(curl -sS -u admin:$password -G https://skafka.azurehdinsight.net/api/v1/clusters/skafka/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2);
```


3. run the following command to *start the kafka producer* in **kafka cluster head node**

```
**you need to create the topic first, maybe in the jupyter notebook with the %%bash magic command**
```
```
java -jar kafka-producer-consumer.jar shenzhen spatial1 wn0-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092,wn1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092 /home/isam/guang.csv 1
```
 - **OR** if you have exported KAFKABROKERS to the *environment variables* like above
```
java -jar kafka-producer-consumer.jar shenzhen spatial1 $KAFKABROKERS /home/isam/guang.csv 1
java -jar kafka-producer-consumer.jar nyc spatial $KAFKABROKERS /home/isam/nyc.csv 1
```
 - kafka java producer takes the following args
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

