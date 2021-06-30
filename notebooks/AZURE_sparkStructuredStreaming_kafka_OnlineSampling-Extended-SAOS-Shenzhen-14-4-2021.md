

```scala
%%configure -f
{
    "conf": {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11",
        "spark.dynamicAllocation.enabled": false
    }
}

```


```scala
/**
 * @Description: extended-SAOS integrated with ApproxSPE , Shenzhen data
 * @author: Isam Al Jawarneh
 * @date: 02/04/2021
 */
```


```scala
//parameters

val sampling_fraction = 0.8
val precision = 30
val error_bound = 0.09
val confidence_level:Float = 90
val Z_alpha_2:Double = confidence_level match {
    case 95  => 1.96
    case 99  => 2.576
    case 68  => 1
    case 90  => 1.645
    case 98  => 2.326
    case _  => 1.96  // the default, catch-all
}
```


```scala
import util.control.Breaks._
import org.apache.spark.sql.streaming.StreamingQueryListener
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
import org.apache.spark.sql.DataFrame
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
```


```scala

```

%%bash
#create topic 'spatial' with 4 partitions
export KafkaZookeepers="zk1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181,zk2-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181"

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 1 --partitions 8 --topic spatial1 --zookeeper $KafkaZookeepers


```scala
%%bash
#list topics to check
export KafkaZookeepers="zk1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181,zk2-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181"
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper $KafkaZookeepers
```


```scala
val kafkaBrokers="wn0-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092,wn1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092"

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
    val geoSeq: Seq[String] = neigh_geohashed_df.select("neighborhood").distinct.rdd.map(r => r(0).asInstanceOf[String]).collect()
    val map = Map(geoSeq map { a => a -> samplingRatio }: _*)

        val tossAcoin = rand(7L)
    val getSamplingRate = udf { (geohash: Any, rnd: Double) =>
      rnd < map.getOrElse(geohash.asInstanceOf[String], 0.0)
    }
val samplepointDF =  points_geohashed_df.filter(getSamplingRate1(map, 0.0)($"neighborhood", tossAcoin))
    return samplepointDF}

```


```scala
def geohashedNeighborhoods(geohashPrecision: Int, filePath: String): DataFrame = 

{

import spark.implicits._
/*preparing the neighborhoods table (static table) .... getting geohashes covering for every neighborhood and 
exploding it, so that each neighborhood has many geohashes*/

// this will be executed only one time - batch mode 
    /*
    you can not use cache here!!!
    */
val rawNeighborhoods = spark.sqlContext.read.format("magellan").option("type", "geojson").load(filePath).select($"polygon", $"metadata"("NAME").as("neighborhood"))//.cache()

val neighborhoods = rawNeighborhoods.withColumn("index", $"polygon" index geohashPrecision).select($"polygon", $"index", 
      $"neighborhood")//.cache()
    print(neighborhoods.count())

val zorderIndexedNeighborhoods = neighborhoods.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoods= neighborhoods.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoods = geohashedNeighborhoods.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }

//unit testing: explodedgeohashedNeighborhoods.show(10)


explodedgeohashedNeighborhoods

}
```

**
## in order to be able to access the Neigboors .geojson file, 
*we need to store it in a blob storage:*
- go to "HDInsight clusters" --> Spark cluster name --> search for "storage accounts",
- select the "Azure Storage" name 
- storage explorer --> blob containers --> sspark --> create new folder "datasets"
- upload shenzhen.geojson
- then you can access it using
- "wasb://sspark@7q6kgdctotuwu.blob.core.windows.net/datasets/shenzhen_converted.geojson"
- where sspark is the spark cluster name
**


```scala


val geohashedNeigboors = geohashedNeighborhoods(precision,"wasb://sspark@7q6kgdctotuwu.blob.core.windows.net/datasets/shenzhen_converted.geojson")
```


```scala

```


```scala
//geohashedNeigboors.where(geohashedNeigboors("geohash") === "ws104u").show()
```


```scala
//geohashedNeigboors.dropDuplicates("geohash").show(2)
```


```scala
val samplepointDF_SSS_join = transformationStream.join(geohashedNeigboors,geohashedNeigboors("geohash")===transformationStream("geohash")).where($"point" within $"polygon")
```


```scala
/*careful here we perform online spatial sampling. this means sampling on-the-fly (OTF) 
as we are taking data directly from transformationStream, which by itself a transformation from a Kafka stream 
'stream'
N.B. we should do the same for binary-tree-stratification (left,right) strata*/
val samplepointDF_SSS = spatialSampleBy(geohashedNeigboors,samplepointDF_SSS_join,sampling_fraction)

```

***
**
then this is a stateful aggregation
mean_deviation is meant to calculate the deviation of Y (speed) from the real mean
before performing a stateful aggregation by applying .groupBy().agg(), any withColumn would have an
effect on a tuple-by-tuple basis, such as the case for mean_deviation
**
***


```scala

val samplingStatisticsDF  = samplepointDF_SSS/*.withColumn("mean_deviation", (col("speed") - lit(batch_mean))*(col("speed") - lit(batch_mean)))*/.groupBy($"neighborhood").agg(
    avg($"speed").as("per_strat_mean"), variance($"speed").as("per_strat_var")//.cast("double"),
    ,sum($"speed").as("per_strat_sum"),/*sum($"mean_deviation").as("mean_dev"),*/
    count($"speed").cast("double").as("per_strat_count")).withColumn("NhYbarh",col("per_strat_mean")* col("per_strat_count")).withColumn("quantity",when($"per_strat_var".isNaN, lit(0)).otherwise((lit(1) - (col("per_strat_count")/(col("per_strat_count")/lit( sampling_fraction)))) * (col("per_strat_count")/lit( sampling_fraction)) * (col("per_strat_count")/lit( sampling_fraction)) * (col("per_strat_var")/col("per_strat_count")))).withColumn("origin_strat_count",col("per_strat_count")/sampling_fraction)

 
```

## Monitoring Streaming Queries
** in this part we monitor the progress of the streaming query by relyin on ` StreamingQueryListener ` **

*the procedure to monitor the progress correctly is this:*
- run this cell, while the streaming query is not active
- run the streamin query
- stop the streaming query
- print the duration in millisecends
[ref](https://databricks.github.io/benchmarks/structured-streaming-yahoo-benchmark/index.html#Spark.html)
***


```scala

@volatile var startTime: Long = 0L //  @volatile
 @volatile var endTime: Long = 0L //  @volatile
@volatile  var numRecs: Long = 0L
 var throughput:Long = 0L//  @volatile
 var totalTimeNoOverhead = 0L //  @volatile

spark.streams.addListener(new StreamingQueryListener() {
  import org.apache.spark.sql.streaming.StreamingQueryListener._
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
      startTime = System.currentTimeMillis

    }
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      endTime = System.currentTimeMillis//System.nanoTime//
   
         }//end onQueryTerminated
    override def onQueryProgress(event: QueryProgressEvent): Unit = {
         numRecs += event.progress.numInputRows

        println("query is making progress!! " )
        }//END onQueryProgress METHOD
  })
```


```scala
//RUN ALL ABOVE
```

***
**
thereafter, we output data to a local in-memory sink
to be able to perform queries locally over already-aggregated stream data.
so, this way we are writing only sumamries in-memory, which is more effecient
**
***


```scala

val points_new = samplingStatisticsDF.writeStream.queryName("queryTable").format("memory").outputMode("complete").start()//outputMode("append")

```


```scala
//check wether the stream is active
points_new.isActive
```


```scala
//points_new.stop
```

***
**
this is a User Defined Function (UDF) so that we can compute a custom formula within spark.sql. we need this 
to calculate the optimal sample size given margin of error e as per the equation below
**
***


```scala


val quantity = (n: Double,nh:Double, N:Double, Nh:Double,V:Double) => {
    
    (n/nh)* scala.math.pow(Nh/N,2) * V
}

val q_func = spark.udf.register("quantity", quantity)
```


```scala
val t2 = System.nanoTime

val monitorProgress = new scala.collection.mutable.ListBuffer[Double]()
val monitorTuplesCount = new scala.collection.mutable.ListBuffer[Double]()
val monitorError = new scala.collection.mutable.ListBuffer[Double]()
val monitorOptimal = new scala.collection.mutable.ListBuffer[Double]()
val monitorDeff = new scala.collection.mutable.ListBuffer[Double]()
val monitorTime = new scala.collection.mutable.ListBuffer[Double]()




var values:scala.collection.mutable.Map[Int,Double] = scala.collection.mutable.Map()
var temp = 0.0
var population_total = 0
var sample_sum:Double = 0
var y_bar = 0.0
val batch_interval = 10

new Thread(new Runnable() {
    override def run(): Unit = {

print(points_new.isActive)

      while (points_new.isActive ) {//start while
          
          val time1 = System.nanoTime
          
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
          
          
          //(n: Double,nh:Double, N:Double, Nh:Double,V:Double)
val newDF = population.withColumn("q",q_func(lit(popTotal_original * sampling_fraction),col("per_strat_count"),lit(popTotal_original),col("origin_strat_count"),when($"per_strat_var".isNaN, lit(0)).otherwise(col("per_strat_var"))))//.withColumn("variance_s",var_func(col("mean_dev")))
newDF.createOrReplaceTempView("deff_view")
val variance_sum = spark.sql("select sum(q) from deff_view").head().getDouble(0)
print(variance_sum)

val optimal_n = scala.math.pow(Z_alpha_2,2) * variance_sum / (scala.math.pow(error_bound,2))
          
          monitorProgress.append(y_bar)
          monitorTuplesCount.append(N)
         monitorError.append(SE_SSS)
   
          
          
           /* sending data from kafka every 1 ms means that we receive 10000 tuples every 
          batch interval. change sleep time here or better to change the ingestion rate in kafka*/
          monitorOptimal.append(optimal_n)
          val time = (System.nanoTime - time1) / 1e9d
          print(time)
          monitorTime.append(time)
          
           if(N>=1155000){
             points_new.stop 
          }
          
          Thread.sleep(10000)
      }//end while
    }
  }).start()
          
```


```scala
val duration2 = (System.nanoTime - t2) / 1e9d
```


```scala
val currentTime = new java.sql.Timestamp(System.currentTimeMillis)
```


```scala
val start = new java.sql.Timestamp(startTime)

```


```scala
val end = new java.sql.Timestamp(endTime)

```


```scala
val duration = endTime - startTime
```


```scala
//calculating the throughput
println(numRecs)
print(numRecs * 1000.0 / duration)
```


```scala
print(numRecs)
```


```scala

println("tuples")
monitorTuplesCount.distinct.foreach(println)
```


```scala
//points_new.stop
```


```scala

println("avg")
monitorProgress.distinct.foreach(println)
```


```scala

println("SE")
monitorError.distinct.foreach(println)
```


```scala

println("optimal_n")
monitorOptimal.distinct.foreach(println)
```


```scala

println("time")
monitorTime.distinct.foreach(println)
```


: 'To purge the Kafka topic, you need to change the retention time of that topic. The default 
retention time is 168 hours, i.e. 7 days. So, you have to change the retention time to 1 second, 
after which the messages from the topic will be deleted. Then, you can go ahead and change the retention 
time of the topic back to 168 hours = 604800000 ms.'



```scala
%%bash
#list topics to check
export KafkaZookeepers="zk1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181,zk2-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181"
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper $KafkaZookeepers

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh  --zookeeper $KafkaZookeepers --alter --topic spatial1 --config retention.ms=604800000


```

%%bash
export kafkaBrokers="wn0-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092,wn1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092"

/usr/hdp/current/kafka-broker/bin/kafka-log-dirs.sh --describe --bootstrap-server $KAFKABROKERS --topic-list spatial1


%%bash
export KafkaZookeepers="zk1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181,zk2-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:2181"

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper $KafkaZookeepers --delete --topic spatial
#sudo systemctl restart kafka
#sudo systemctl status kafka


```scala

```
