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
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._

import scala.util.parsing.json._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object Detector {

  val modelFile = "/user/fsainz/DATA/out/clusters.model"
  val topics = "ecg"
  val topicsSet = topics.split(",").toSet
  val brokers ="10.132.0.3:9091,10.132.0.4:9091"

  val WINDOW = 32
  var windowingF  =  new Array[Double](WINDOW)   //differs of Array[Double](WINDOW) uahhh!!

  for ( i <- 0 until WINDOW) {
    val y = Math.sin(Math.PI * i / (WINDOW - 1.0));
    windowingF(i) = y * y
  }


  def process ( wSize: Int, frame : Array[Double] , model:KMeansModel) : Array[Double] = {

    // Split frame in WINDOW size windows
    val frameLength = frame.length

    assert( frameLength % wSize == 0 )  // should throws a exception if not

    val signalWindows = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]
    val codecWindows = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]
    val ret = scala.collection.mutable.ArrayBuffer.empty[Double]
    val nWindows:Int = frameLength / wSize

    for ( i <- 0 until (nWindows * 2)-1) {   // we step 1/2 wSize
      var p = i*wSize/2
      var signalWindow = frame.slice(p, p+wSize  )

      var tapWindow = (signalWindow,DataFrame.windowingF).zipped.map(_*_)   // multiple vector item by item
      val wNorm = Vectors.norm(Vectors.dense(tapWindow) , 2)
      tapWindow = tapWindow.map( x => x/wNorm)

      val nc = model.predict(Vectors.dense(tapWindow))

      val cluster = model.clusterCenters(nc)

      val codecWindow = cluster.toArray.map(x => x*wNorm)   // scale to signal

      signalWindows.append(signalWindow)
      codecWindows.append(codecWindow.toArray)
      // Coded windows has tapering function scale. get it of it using a trick sum two displacements halfs.
      // see properties or tapering function.
      // we produce half a window a time
      // and we have to arrange in some way first and last halfs
      var half1 = new Array[Double](wSize)
      var half2 = new Array[Double](wSize)
      if (i == 0) {
        half1 = codecWindows(i).slice(0,wSize/2)
        half2 = half1
      }
      else {
        print(","+i+",")
        half1= codecWindows(i-1).slice(wSize/2,wSize)
        half2= codecWindows(i).slice(0,wSize/2)

      }
      var r = (half1 , half2).zipped.map(_+_)   // sum halfs
      ret ++= r
      // last half window
      if ( i == (nWindows*2) -2 ) {
        half1= codecWindows(i).slice(wSize/2,wSize)
        half2= half1
        print ("*************LAST + " + i)
        r = (half1 , half2).zipped.map(_+_)   // sum halfs
        ret ++= r
      }
    }
    ret.toArray
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(" Running EGC anomaly Detector over kafka streams")
    // create spark context
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("ecg-a-detector")
    val sc = new SparkContext(conf)
    //val sparkConf = new SparkConf().setAppName("ecg-a-detector")
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
equals()

    val records = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val events = records.map( record =>
    { val evtStr = record.value().toString
      val evtMap = JSON.parseFull(evtStr).getOrElse("")
        .asInstanceOf[Map[String, Any]]
      val frame =   evtMap.getOrElse("data", List[Double]()).asInstanceOf[List[Double]]
      val ts =      evtMap.getOrElse("ts",0.0)
      val SeqInt =  evtMap.getOrElse("seqInt",0)
      val srcId  =  evtMap getOrElse("srcId","")
      val codedFrame = process(WINDOW, frame.toArray, clusters)
      //val codedFrame = frame
      println(codedFrame.getClass.getSimpleName)
      ( srcId, SeqInt,ts,frame.toArray , codedFrame,codedFrame.getClass.getSimpleName, 1) } )
    events.print

    //records.map(record => (record.toString) ).print



    ssc.start()
    //ssc.awaitTerminationOrTimeout(60)
    ssc.awaitTermination()

    ssc.stop()


  }


}
