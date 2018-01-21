/**
* Created by fsainz. felix.sainz@gmail.com
*
* SUMMARY:
* EXECUTION:
* spark-submit --class ecg.Detector --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0  ecg-detector_2.11-1.0.jar
*/
package ecg


import fxo.utils.{ BigQueryDB, Timeutils}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.Instant

object  Detector {
  val bachTime = Seconds(10)



  val outputPath = "/user/fsainz/data/out/coded/"
  val topics = "ecg"
  val topicsSet = topics.split(",").toSet
  val brokers ="10.132.0.3:9091,10.132.0.4:9091"

  val wSize = ECGframe.windowSize  //120 32 or 40

  val patientID = "a02"  //"105s1"
  val WindowsPerFrame = 30 // 32 or 150
  val anomalyTheshold =   1
  val trStep = (wSize / 10).toInt

  val saveInterval = Seconds(60)


  val hdfsOutPath = "/user/fsainz/data/out/"

  val ModelFileName = hdfsOutPath + "dict-" + patientID +  "_W" + wSize + "-" + trStep +".LE.model"




  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    println(" Running EGC anomaly Detector over kafka streams")
    // create spark context
    // The master requires 2 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ecg-a-detector")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, bachTime)

    // val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> "consumer1",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val clusters = KMeansModel.load(sc, ModelFileName)

    // kafka Raw Data
    val records = KafkaUtils.createDirectStream[String, String](
      ssc,
     //LocationStrategies.PreferConsistent,
      LocationStrategies.PreferBrokers, // use it to get partition leader distribution.
           ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )
    // get data and reorder. ready to save

    var evtTs :Instant = Instant.now() // it will be overrided for each batch


    val events = records.map( record => {

      println("****************************************************")
      println("Using TsNow" + evtTs.toEpochMilli )
      val evtStr = record.value().toString
      // split json record on its fields
      //println(evtStr)
      val( srcId, srcTs, seqTref, noscaledframe)  =  JsonHelpers.json2list(evtStr) //JsonHelpers.json2list(evtStr)
      // scale the ecg data
      val frame = noscaledframe.map( _ * ECGframe.scale)
      // coded ecg data using kmeans  dictionary
      val codedFrame = ECGframe.process(ECGframe.windowSize, frame.toArray, clusters)

      // get loss error.
      /* There is an error coding frames borders, discard it at the moment*/
      /*
      val loss = (frame, codedFrame).zipped.map(_-_)   // sustract original codded frame from original one
      */


      val p1 = wSize / 2
      val p2 = frame.length - p1
      val loss = (frame.slice(p1, p2), codedFrame).zipped.map(_-_)   // subtract coded frame from original one

      //println(codedFrame.getClass.getSimpleName)
      //( srcId, SeqInt,ts,frame.toArray , codedFrame, frameError, 1)
      val maxLoss = loss.max
      val csvLine = ";" + frame.slice(p1, p2).mkString(",") + ";" + codedFrame.mkString(",") + ";" + loss.mkString(",") + ";"

      val dateTimePath = Timeutils.datePath(evtTs)
      val timePathStr = dateTimePath._1  + dateTimePath._2
       ( srcId, srcTs, seqTref, maxLoss, timePathStr,csvLine)
      } )
    //events.print()

     val toSavedAgreggation = events.window( saveInterval, saveInterval)


    // log to hdfs each event received..
    toSavedAgreggation.foreachRDD ( rdd => {
      // Following code is executed in the driver

      if(!rdd.isEmpty()) {

        println("rdd is not empty: " + rdd.count )
       //----------------ends driver execution
        // following is a executor running code
        val tic = System.currentTimeMillis()
        println("-----------------------------------------------------------------")
        val dateTimePath = Timeutils.datePath(evtTs)
        val timePathStr = dateTimePath._1  + dateTimePath._2
         // add storagepath to tuples to refences among Bigquery Db and hdfs I
        val filesPath = outputPath + timePathStr  // splits path in minutes_ms
       //rdd.map( x => (x._1, x._2)).saveAsTextFile(filesPath)   //this will overwrite for each rdd (bach of data)

        rdd.saveAsTextFile(filesPath)
        val tac = System.currentTimeMillis()
        println("Write to file Took ms:" + (tac-tic))
        evtTs = Instant.now()                     // get timestamp for each batch time.
        println("****************************************************")
        println("New ref time for next window time events time rdd @_" + evtTs.toEpochMilli)

      }
    })

   // Save to Big Query only anomalies

    val save2db = events.filter(e => e._4 > anomalyTheshold ).map{ e =>
      val jsonObject = JsonHelpers.list2json(e)
      // bigquery
      // IndirectBigQueryOutputFormat discards keys, so set key to null. bellow
      (null,jsonObject )
    }


    // BigqueryDB configuration
    val inputTableId = null // We are not reading nothing from bigquery.
    val outputTableId = ":ecg.anomalies"
    val dbConf = BigQueryDB.createConnection(sc, inputTableId, outputTableId)

    save2db.foreachRDD ( rdd => {
      val tic = System.currentTimeMillis()

      // empty data raise an exception
      if ( ! rdd.isEmpty()) {
        println("\nsave2db not Empty")
        rdd.saveAsNewAPIHadoopDataset(dbConf)
      }
      val tac = System.currentTimeMillis()
      println("\nTook ms:" + (tac-tic))
    })



    ssc.start()
    //ssc.awaitTerminationOrTimeout(60)
    ssc.awaitTermination()

    ssc.stop()


  }


}
