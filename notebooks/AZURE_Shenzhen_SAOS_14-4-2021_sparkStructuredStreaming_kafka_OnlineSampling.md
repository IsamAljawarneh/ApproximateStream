
# approximate spatial query processing on Microsoft Azure with Apache Spark and Kafka

# configurations
***

***

**you need to import the following libraries**
```
- org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0
- com.esri.geometry:esri-geometry-api:1.2.1
- commons-io:commons-io:2.6
- org.apache.spark:spark-streaming_2.11:2.2.0
```
* you should do this in the `"spark.jars.packages"` section of the  `%%configure -f` spark magic cell below
* for the `magellan` spatial library, you need to upload the fat .jar file to the `storage account` `container` of your Azure Spark cluster [instructions here](https://github.com/IsamAljawarneh/ApproximateStream/blob/master/instructions/run_on_Azure.md)
    - suppose you have uploaded that to a folder titled `jars`, OR replace the `FOLDER_NAME` with the folder name where you have placed magellan`
    - then use the directive `"spark.jars"` section of the  `%%configure -f` spark magic cell below to import this library
    - replace `CONTAINER_NAME` with the container name in your Spark storage account where you hosted the `magellan` spatial library. ALSO, replace `STORAGE_ACCOUNT_NAME` with the name of your Spark `storage account`


```scala
%%configure -f
{
    "conf": {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0",
        "spark.jars":"wasbs://CONTAINER_NAME@STORAGE_ACCOUNT_NAME.blob.core.windows.net/FOLDER_NAME/magellan-1.0.5-s_2.11.jar",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11",
        "spark.dynamicAllocation.enabled": false
    }
}

```


```scala

```


```scala
/**
 * @Description: Approximate Spatial Query Processing on Azure with Spark and Kafka
 * @author: Isam Al Jawarneh
 * @date: 02/04/2021
 */
```

# parameters


```scala
//parameters

val sampling_fraction = 0.2
val precision = 25

```

# import


```scala
import util.control.Breaks._
import org.apache.spark.sql.streaming.StreamingQueryListener
//import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter
import magellan._
import magellan.index.ZOrderCurve
import magellan.{Point, Polygon}

import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import java.io.{BufferedWriter, FileWriter}
import org.apache.commons.io.FileUtils
import java.io.File
import scala.collection.mutable.ListBuffer
import java.time.Instant
//import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.sql.DataFrame
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
```

# creating a Kafka topic
* in the following cell, we create a kafka topic
    - replace `TOPIC_NAME` with the name of topic of your choice, i used `spatial1` here
    - replace `HOST_INFO` with the host information you gathered using jq, that is a list that should end with `2181` for each element [instructions here](https://github.com/IsamAljawarneh/ApproximateStream/blob/master/instructions/run_on_Azure.md)
    - you gather the information with the following command sequence
    ```
export password='KAFKA_CLUSTER_PASS'
export CLUSTERNAME=KAFKA_CLUSTER_NAME
```
`
    curl -sS -u admin:$password -G "https://skafka.azurehdinsight.net/api/v1/clusters/skafka/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2
`
    - replace `KAFKA_CLUSTER_PASS` with your kafka clusetr passs, and `KAFKA_CLUSTER_NAME` with you kafka cluster name
- N.B **you create the topic once and disable the cell**


```scala
%%bash

#create topic 'spatial1' with 16 partitions 
export KafkaZookeepers="HOST_INFO"

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 1 --partitions 16 --topic TOPIC_NAME --zookeeper $KafkaZookeepers

```


```scala
%%bash
#list topics to check
export KafkaZookeepers="HOST_INFO"
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper $KafkaZookeepers
```

# Kafka brokers
* get kafka brokers (ending with 9092) [instructions here](https://github.com/IsamAljawarneh/ApproximateStream/blob/master/instructions/run_on_Azure.md)


```scala
val kafkaBrokers="KAFKA_BROKERS"

```


```scala

val stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("startingOffsets", "earliest").option("subscribe", "spatial1").load()//.option("maxOffsetsPerTrigger",2).option("startingOffsets", "earliest")
```


```scala
val schemaNYCshort = StructType(Array(
    StructField("id", StringType, false),
    StructField("lat", DoubleType, false),
    StructField("lon", DoubleType, false),
    StructField("time", StringType, false),
    StructField("speed", DoubleType, false)))
```


```scala
val geohashUDF = udf{(curve: Seq[ZOrderCurve]) => curve.map(_.toBase32())}
```


```scala
val transformationStream1 = stream.selectExpr("CAST(value AS STRING)").as[(String)].select(from_json($"value", schemaNYCshort).as("data")).select("data.*")

val ridesGeohashed = transformationStream1.withColumn("point", point($"lat",$"lon")).withColumn("index", $"point" index  precision).withColumn("geohashArray", geohashUDF($"index.curve")).select( $"point",$"geohashArray",$"speed")
val dataStream1 = ridesGeohashed.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }
val transformationStream = dataStream1.select("point","speed", "geohash")//.groupBy("geohash").count().orderBy($"count".desc) //
```


```scala

 def getSamplingRate1(map: Map[String, Double], defaultValue: Double) = udf{
  (geohash: String, rnd: Double) =>
      rnd < map.getOrElse(geohash.asInstanceOf[String], 0.0)
}

def spatialSampleBy(neigh_geohashed_df:DataFrame, points_geohashed_df:DataFrame, samplingRatio: Double): DataFrame = {
    val geoSeq: Seq[String] = neigh_geohashed_df.select("geohash").distinct.rdd.map(r => r(0).asInstanceOf[String]).collect()
    val map = Map(geoSeq map { a => a -> samplingRatio }: _*)

        val tossAcoin = rand(7L)
    val getSamplingRate = udf { (geohash: Any, rnd: Double) =>
      rnd < map.getOrElse(geohash.asInstanceOf[String], 0.0)
    }
val samplepointDF =  points_geohashed_df.filter(getSamplingRate1(map, 0.0)($"geohash", tossAcoin))
    return samplepointDF}

```


```scala
def geohashedNeighborhoods(geohashPrecision: Int, filePath: String): DataFrame = 

{

import spark.implicits._
/*preparing the neighborhoods table (static table) .... getting geohashes covering for every neighborhood and 
exploding it, so that each neighborhood has many geohashes*/

// this will be executed only one time - batch mode 
val rawNeighborhoods = spark.sqlContext.read.format("magellan").option("type", "geojson").load(filePath).select($"polygon", $"metadata"("NAME").as("neighborhood"))//.cache()

val neighborhoods = rawNeighborhoods.withColumn("index", $"polygon" index geohashPrecision).select($"polygon", $"index", 
      $"neighborhood")//.cache()
    //print(neighborhoods.count())

val zorderIndexedNeighborhoods = neighborhoods.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoods= neighborhoods.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoods = geohashedNeighborhoods.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }

//unit testing: explodedgeohashedNeighborhoods.show(10)


explodedgeohashedNeighborhoods

}
```

# retreiving the regions file
* replace `CONTAINER_NAME` with the container name in your Spark storage account where you hosted the `magellan` spatial library. ALSO, replace `STORAGE_ACCOUNT_NAME` with the name of your Spark `storage account` 


```scala

val geohashedNeigboors = geohashedNeighborhoods(precision,"wasb://CONTAINER_NAME@STORAGE_ACCOUNT_NAME.blob.core.windows.net/datasets/shenzhen_converted.geojson")
```


```scala
//geohashedNeigboors.dropDuplicates("geohash").show(2)
geohashedNeigboors.count()
```


```scala
//val population = spark.sql("select * from queryTable")
```


```scala

val samplepointDF_SSS = spatialSampleBy(geohashedNeigboors,transformationStream,sampling_fraction)

```


```scala
//run all above
```

## ramp-up: generate some records for kafka before running this cell
- [instructions here](https://github.com/IsamAljawarneh/ApproximateStream/blob/master/instructions/run_on_Azure.md) 


```scala

val samplingStatisticsDF  = samplepointDF_SSS.groupBy($"geohash").agg(
    avg($"speed").as("per_strat_mean"), variance($"speed").as("per_strat_var")//.cast("double"),
    ,sum($"speed").as("per_strat_sum"),
    count($"speed").cast("double").as("per_strat_count")).withColumn("NhYbarh",col("per_strat_mean")* col("per_strat_count")).withColumn("quantity",when($"per_strat_var".isNaN, lit(0)).otherwise((lit(1) - (col("per_strat_count")/(col("per_strat_count")/lit( sampling_fraction)))) * (col("per_strat_count")/lit( sampling_fraction)) * (col("per_strat_count")/lit( sampling_fraction)) * (col("per_strat_var")/col("per_strat_count")))).withColumn("origin_strat_count",col("per_strat_count")/sampling_fraction)

 
```

## before running this cell run the kafka producer in the kafka cluster head node


```scala
//run the kafka-java-producer before running this cell
/*
thereafter, we output data to a local in-memory sink
to be able to perform queries locally over already-aggregated stream data.
so, this way we are writing only sumamries in-memory, which is more effecient
*/
val points_new = samplingStatisticsDF.writeStream.queryName("queryTable").format("memory").outputMode("complete").start()//outputMode("append")

```


```scala
//check wether the stream is active
points_new.isActive
```


```scala
//points_new.stop
```


```scala

val monitorProgress = new scala.collection.mutable.ListBuffer[Double]()
val monitorTuplesCount = new scala.collection.mutable.ListBuffer[Double]()
val monitorError = new scala.collection.mutable.ListBuffer[Double]()


var values:scala.collection.mutable.Map[Int,Double] = scala.collection.mutable.Map()
var temp = 0.0
var population_total = 0
var sample_sum:Double = 0
var y_bar = 0.0
val batch_interval = 10

new Thread(new Runnable() {
    override def run(): Unit = {

print(points_new.isActive)

      /*while (!points.isActive) {
          Thread.sleep(100)
        }*/
      while (points_new.isActive ) {//start while
          
          
val population = spark.sql("select * from queryTable")
population.createOrReplaceTempView("updates")

val tau_hat_str  = spark.sql("select sum(NhYbarh) from updates").head().getDouble(0)
val popTotal_from_sampling  = spark.sql("select sum(per_strat_count) from updates").head().getDouble(0)

 val y_bar = tau_hat_str/popTotal_from_sampling
val N = popTotal_from_sampling/sampling_fraction
print(y_bar)
          
val estimated_varianceS_estimated_total  = spark.sql("select sum(quantity) from updates").head().getDouble(0)
val popTotal_original: Double = spark.sql("select sum(per_strat_count) from updates").head().getDouble(0)/sampling_fraction
val estimated_varianceS_estimated_Mean:Double = estimated_varianceS_estimated_total/(popTotal_original*popTotal_original)
val SE_SSS:Double = scala.math.sqrt(estimated_varianceS_estimated_Mean)

          
          monitorProgress.append(y_bar)
          monitorTuplesCount.append(N)
         monitorError.append(SE_SSS)
          
      
          
          if(N>=1155000){
             points_new.stop 
          }
          
          Thread.sleep(10000)
      }//end while
    }
  }).start()
          

```


```scala

println("tuples")
monitorTuplesCount.distinct.foreach(println)

```


```scala
println("avg")
monitorProgress.distinct.foreach(println)

```


```scala
println("Standard Error")
monitorError.distinct.foreach(println)

```

## : 'To purge the Kafka topic, 
you need to change the retention time of that topic. The default 
retention time is 168 hours, i.e. 7 days. So, you have to change the retention time to 1 second, 
after which the messages from the topic will be deleted. Then, you can go ahead and change the retention 
time of the topic back to 168 hours = 604800000 ms.'

**after you change the retention to 1000 ms, it takes some time to empty the topic, so, if you describe the topic you may still see the partitions not empty for some time**


```scala
%%bash
#list topics to check
export KafkaZookeepers="HOST_INFO"
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper $KafkaZookeepers

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh  --zookeeper $KafkaZookeepers --alter --topic spatial1 --config retention.ms=604800000

```


```scala

```
