/**
  * Created by fsainz. felix.sainz@gmail.com
  *
  * SUMMARY:
  * OTHER:
  * spark-submit --class Detector --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0  ecg-detector_2.11-1.0.jar
  */

import org.apache.log4j.Logger
import org.apache.log4j.Level


import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import scala.util.parsing.json._

object Detector {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(" Running EGC anomaly Detector over kafka streams")
    // create spark context
    // The master requires 2 cores to prevent from a starvation scenario.
    val sc = new SparkConf().setMaster("local[2]").setAppName("ecg-a-detector")
    //val sparkConf = new SparkConf().setAppName("ecg-a-detector")
    val ssc = new StreamingContext(sc, Seconds(2))
    val topics = "ecg"
    val topicsSet = topics.split(",").toSet
    val brokers ="10.132.0.3:9091,10.132.0.4:9091"
    // val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> "consumer1",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val records = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val events = records.map( record =>
    { val evtStr = record.value().toString
      val evtMap = JSON.parseFull(evtStr).getOrElse("")
        .asInstanceOf[Map[String, Any]]
      val data =   evtMap.getOrElse("data", List[Double]()).asInstanceOf[List[Double]]
      val ts =     (evtMap get "ts").getOrElse(0.0)
      val SeqInt = (evtMap get "seqInt").getOrElse(0)
      val srcId  = (evtMap get "srcId").getOrElse("")
      ( srcId, SeqInt,ts,data , 1) } )
    events.print
    records.map(record => (record.toString) ).print
    ssc.start()
    //ssc.awaitTerminationOrTimeout(60)
    ssc.awaitTermination()

    ssc.stop()


  }

}
