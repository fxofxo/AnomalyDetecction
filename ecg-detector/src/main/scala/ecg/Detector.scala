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

object Detector {
  val bachTime = Seconds(2)
  val saveInterval = Seconds(120)


  val outputPath = "/user/fsainz/data/out/coded/"
  val topics = "ecg-frame"
  val topicsSet = topics.split(",").toSet
  val brokers ="10.132.0.3:9091,10.132.0.4:9091"

  val wSize = ECGframe.windowSize  //120 32 or 40

  val anomalyTheshold =    .30
  val trStep = (wSize / 10).toInt

  val hdfsOutPath = "/user/fsainz/data/out/"


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    println("\n\nRunning EGC anomaly Detector over kafka streams")
    println("Reading data from kakfa topic:" + topics)
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
    // it is not posible to load dinamically de kmeans model...so create dictionary statically
    val patients = Array("105", "106", "108", "116", "119","200","201","222")

    var kmodels = scala.collection.mutable.Map[String, KMeansModel]()
    for ( patientID <- patients)
      {
        val ModelFileName = hdfsOutPath + "dict-" + patientID +  "_W" + wSize + "-" + trStep +".LE.model"
        val model = KMeansModel.load(sc, ModelFileName)
        kmodels += (patientID  -> model)
      }

    // kafka Raw Data
    val records = KafkaUtils.createDirectStream[String, String](
      ssc,
     //LocationStrategies.PreferConsistent,
      LocationStrategies.PreferBrokers, // use it to get partition leader distribution.
           ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )
    // get data and reorder. ready to save

    var evtTs :Instant = Instant.now() // it will be overrided for each batch
    var dateTimePath = Timeutils.datePath(evtTs)
    var timePathStr = dateTimePath._1   // + dateTimePath._2

    val events = records.map( record => {

      println(" \n F ======================================================================")

      print("\n Will be Save on path: " + timePathStr + "   -(" + evtTs.toEpochMilli +")" )
      val tic = System.currentTimeMillis()
      val evtStr = record.value().toString
      // split json record on its fields
      //println(evtStr)
      val( srcId, srcTs, seqTref, noscaledframe)  =  JsonHelpers.json2list(evtStr) //JsonHelpers.json2list(evtStr)
      // scale the ecg data
      val frame = noscaledframe.map( _ * ECGframe.scale)
      // coded ecg data using kmeans  dictionary
      val patientID = srcId.split('/')(2).split('.')(0)

      var dict :KMeansModel = null

     try{
           dict = kmodels(patientID)
      }
      catch{
        case e: java.util.NoSuchElementException =>   {

         println( "\nNot found k-means model for patient ***************")
        }
      }

      val codedFrame = ECGframe.process(ECGframe.windowSize, frame.toArray, dict)

      // get loss error.
      /* There is an error coding frames borders, discard it at the moment*/
      /*
      val loss = (frame, codedFrame).zipped.map(_-_)   // subtract original codded frame from original one
      */

      val p1 = wSize / 2
      val p2 = frame.length - p1
      val loss = (frame.slice(p1, p2), codedFrame).zipped.map(_-_)   // subtract coded frame from original one

      //println(codedFrame.getClass.getSimpleName)
      //( srcId, SeqInt,ts,frame.toArray , codedFrame, frameError, 1)
      val sqrMinLoss = math.pow(loss.min,2)
      val sqrMaxLoss = math.pow(loss.max,2)
      val maxAbsError = math.sqrt( math.max( sqrMaxLoss,sqrMinLoss) )
      val csvLine = ";" + frame.slice(p1, p2).mkString(",") + ";" + codedFrame.mkString(",") + ";" + loss.mkString(",") + ";"
      val tac =  System.currentTimeMillis()

      println("\n\nFrame Process time = " + (tac-tic))
       ( srcId, srcTs, seqTref, maxAbsError, timePathStr,csvLine)
      } )  // records map = events



    //events.print()

     val toSavedAgreggation = events.window( saveInterval, saveInterval)


    // log to hdfs each event received..
    toSavedAgreggation.foreachRDD ( rdd => {
      // Following code is executed in the driver

      if(!rdd.isEmpty()) {

        println("There are frames to save : " + rdd.count )
       //----------------ends driver execution
        // following is a executor running code
        val tic = System.currentTimeMillis()
        println("-----------------------------------------------------------------")

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
        println("\nsave2db not Empty, ")
        rdd.saveAsNewAPIHadoopDataset(dbConf)
        val tac = System.currentTimeMillis()
        print("\nTook ms:" + (tac-tic))
      }

    })



    ssc.start()
    //ssc.awaitTerminationOrTimeout(60)
    ssc.awaitTermination()

    ssc.stop()


  }


}
