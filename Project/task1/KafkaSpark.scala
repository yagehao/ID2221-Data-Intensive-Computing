package sparkstreaming

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
// import org.apache.spark.sql.catalyst.package

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


case class percentage(key: String, per1: Double, per2: Double)
object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka 
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    // init cassandra
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // connect to Cassandra and make a keyspace and table
    session.execute("CREATE KEYSPACE IF NOT EXISTS covid WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS covid.status (key text, per1 float, per2 float, PRIMARY KEY(key));")
    
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("COVID1")
      .getOrCreate()

    // read (key, value) pairs from kafka
    var df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("Subscribe", "covid")
      .load()

    import spark.implicits._
    // extract key-value frospark.implicits._m (null, "a, 1")
    val res = df.withColumn("_tmp", split(col("value"),"\\,")).select(
        $"_tmp".getItem(1).as("FirstDose"),
        $"_tmp".getItem(3).as("SecondDose"),
        $"_tmp".getItem(7).as("Population"),
        $"_tmp".getItem(8).as("key"),
        $"_tmp".getItem(9).as("TargetGroup"),
        $"_tmp".getItem(6).as("Region")
        ).where("TargetGroup = 'ALL' AND Region = key")
    // println(lines)
    println(res)


    // measure the average value for each key in a stateful manner
    import spark.implicits._
  
    def mappingFunc(key: String, values: Iterator[Row], state: GroupState[(Double,Double, Double)]): Iterator[percentage] = {
      var (sum1,sum2,popu) = state.getOption.getOrElse((0.0,0.0,0.0))
      values.foreach{ x=>
       // println(x)
        sum1 =  x.getString(0).toDouble + sum1 
        sum2 =  x.getString(1).toDouble + sum2 
        popu =  x.getString(2).toDouble 
      }
  
      state.update((sum1,sum2,popu))
      val per1 = sum1/popu
      val per2 = sum2/popu
      Iterator(percentage(key,per1,per2))
    }
    
    val result = res.groupByKey(x=>x.getString(3)).flatMapGroupsWithState(
      outputMode= OutputMode.Append(),
      GroupStateTimeout.ProcessingTimeTimeout())(func = mappingFunc _)
    // val result = spark.sql("select _1 as key, _2 as value from tmp")
    println(result)


    // // output datastream to console
    // val query = result.writeStream
    //   .format("console")
    //   .outputMode("append")
    //   .start()

    // // disk sink
    //  val query = result.writeStream
    //   .format("json")
    //   // .option("path","/home/osboxes/Project/task1/result1")
    //   .option("path","./result1")
    //   .option("checkpointLocation","./checkpoint1")
    //   .outputMode("append")
    //   .start()

    // cassandra sink
    val query= result.writeStream
       .foreachBatch((batchDF, batchId) =>
        batchDF.write
               .format("org.apache.spark.sql.cassandra")
               .mode("append")
               .options(Map("keyspace" -> "covid", "table" -> "status"))
               .save())
      .trigger(Trigger.ProcessingTime(3000))
      .option("checkpointLocation", "./checkpoint1")
      .start

    query.awaitTermination()
  }
}
