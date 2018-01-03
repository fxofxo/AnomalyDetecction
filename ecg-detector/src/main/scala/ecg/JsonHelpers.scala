package ecg

import scala.util.parsing.json.{JSONObject,JSON}

object JsonHelpers {

  def list2json(frame: (String, Double, Double, Double, String, String)): JSONObject = {
    // frame  (srcId, srcTref , qTs, SeqInt,maxloss,csvLine )
    val srcId = frame._1
    val srcTs = frame._2
    val frameRef = frame._3
    val maxLoss = frame._4
    val timePathStr = frame._5
    val data = frame._6
    val jsonObject = new JSONObject(Map(
      "srcId" -> srcId,
      "srcTs" -> srcTs,
      "frameRef" -> frameRef,
      "maxLoss" -> maxLoss,
      "timePathRef" -> timePathStr,
      "dataSlice" -> data.slice(0,60)  // save only part of the data just to easy debugging
    ))
    jsonObject
  }

  def json2list(evtStr: String):(String, Double, Double, List[Double]) =
  {
    val evtMap = JSON.parseFull(evtStr).getOrElse("").asInstanceOf[Map[String, Any]]

    val srcId = evtMap.getOrElse("srcId", "").asInstanceOf[String]
    val srcTs = evtMap.getOrElse("srcTs", 0.0).asInstanceOf[Number].doubleValue
    val frameRef = evtMap.getOrElse("frameRef", 0.0).asInstanceOf[Number].doubleValue
    val frame = evtMap.getOrElse("data", List[Double]()).asInstanceOf[List[Double]]

    (srcId, srcTs, frameRef, frame)
  }
}
