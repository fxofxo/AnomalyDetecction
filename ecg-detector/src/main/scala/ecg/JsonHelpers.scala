package ecg

import scala.util.parsing.json.JSONObject


object JsonHelpers{

  def frame2json(frame: (String, Any, Any, Any)): JSONObject = {
    // frame  (csvLine , srcId, ts, SeqInt)

    val data = frame._1
    val srcId = frame._2
    val ts = frame._3.asInstanceOf[Number].longValue
    val seqInt = frame._4
    val jsonObject = new JSONObject(Map("data" -> data,
                                    "srcId" -> srcId,
                                    "ts" -> ts,
                                    "seqInt" -> seqInt))
    jsonObject
  }

}

