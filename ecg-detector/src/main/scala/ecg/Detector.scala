package ecg

import com.google.gson.JsonObject
import fxo.utils.BigQueryDB
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Detector {

  val modelFile = "/user/fsainz/data/out/clusters-a02.model"
  val outputPath = "/user/fsainz/data/out/coded"
  val topics = "ecg"
  val topicsSet = topics.split(",").toSet
  val brokers ="10.132.0.3:9091,10.132.0.4:9091"

  val WINDOW = 32
  var windowingF  =  new Array[Double](WINDOW)   //differs of Array[Double](WINDOW) uahhh!!

  for ( i <- 0 until WINDOW) {
    val y = Math.sin(Math.PI * i / (WINDOW - 1.0));
    windowingF(i) = y * y
  }
  // Helper to convert (word, count) tuples to JsonObjects.
  def convert2Json(pair: (String, Long)) : JsonObject = {
    val word = pair._1
    val count = pair._2
    val jsonObject = new JsonObject()
    jsonObject.addProperty("word", word)
    jsonObject.addProperty("word_count", count)
    return jsonObject
  }

  //def convertToJson(record: (String, Any, Any, Any)) = ???

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(" Running EGC anomaly Detector over kafka streams")
    // create spark context
    // The master requires 2 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ecg-a-detector")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> "consumer1",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val clusters = KMeansModel.load(sc, modelFile)


    val records = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )

    val events = records.map( record => {
      val evtStr = record.value().toString
      val evtMap = JSON.parseFull(evtStr).getOrElse("").asInstanceOf[Map[String, Any]]
      val frame =   evtMap.getOrElse("data", List[Double]()).asInstanceOf[List[Double]]
                      .map(_*ecgFrame.scale)
      val ts =      evtMap.getOrElse("ts", 0.0)
      val SeqInt =  evtMap.getOrElse("seqInt",0)
      val srcId  =  evtMap getOrElse("srcId","")
      val codedFrame = ecgFrame.process(ecgFrame.windowSize, frame.toArray, clusters)
      val loss = (frame, codedFrame).zipped.map(_-_)   // sustract original codded frame from original one

      //println(codedFrame.getClass.getSimpleName)
      //( srcId, SeqInt,ts,frame.toArray , codedFrame, frameError, 1)
      val csvLine = frame.mkString(",") + ";" + codedFrame.mkString(",") + ";" + loss.mkString(",") + ";"

      //( frame.slice(0,10), codedFrame.slice(0,10).toList, loss.slice(0,10), frame.length, srcId, SeqInt, ts )
      (csvLine , srcId,ts, SeqInt)
      } )
    //events.print()
    events.count.foreachRDD{  countRdd =>
      println("for eachRdd")
      println("countRdd collect =" + countRdd.collect.toString +"||")
    }
    var counter = 0

    // log each event received..
    events.foreachRDD ( rdd => {
      val evtTs = System.currentTimeMillis
      println("hello events rdd @-" + evtTs)  // execute in the driver

      if(!rdd.isEmpty()) {
        counter += 1         //using this from
        println("rdd is not empty: " + rdd.count + "||" + counter + "||")
        rdd.map( x => (x._1)).saveAsTextFile(outputPath+counter)                          //this will overwrite for each rdd (bach of data)
      }
    })


    // Input parameters.
    val inputTableId = "publicdata:samples.shakespeare"
    val outputTableId = ":wordcount_dataset.wordcount_output"

    // Write data back into a new BigQuery table.
    // IndirectBigQueryOutputFormat discards keys, so set key to null. bellow



    val save2db = events.map( e => (null, convert2Json((e._1,e._3.asInstanceOf[Number].longValue))))

    save2db.foreachRDD ( rdd => {
      val dbConf = BigQueryDB.createConnection(sc, inputTableId, outputTableId)

      rdd.saveAsNewAPIHadoopDataset(dbConf)
    })



    /*events.foreachRDD { rdd =>

      rdd.foreachPartition { pRecords =>
        val dbConf = BigQueryDB.createConnection(sc, inputTableId, outputTableId)
        pRecords.
        pRecords.foreach(record =>  (null, convertToJson(("ll",3))).saveAsNewAPIHadoopDataset(dbConf))
      }
    }
    */


    ssc.start()
    //ssc.awaitTerminationOrTimeout(60)
    ssc.awaitTermination()

    ssc.stop()


  }


}
