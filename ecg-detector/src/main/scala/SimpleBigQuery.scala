import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat


object SimpleBigQuery {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(" Running EGC anomaly Detector over kafka streams")
    // create spark context
    // The master requires 2 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ecg-a-detector")
    val sc = new SparkContext(sparkConf)


    import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
    import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat
    import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat
    import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration
    import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat
    import com.google.gson.JsonObject
    import org.apache.hadoop.io.LongWritable
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

    @transient
    val conf = sc.hadoopConfiguration


    // Input parameters.
    val fullyQualifiedInputTableId = "publicdata:samples.shakespeare"
    val projectId = conf.get("fs.gs.project.id")
    val bucket = conf.get("fs.gs.system.bucket")

    println(projectId, bucket)


    // Input configuration.
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
    BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)


    // Output parameters.
    val outputTableId = projectId + ":wordcount_dataset.wordcount_output"
    // Temp output bucket that is deleted upon completion of job.
    val outputGcsPath = ("gs://" + bucket + "/hadoop/tmp/bigquery/wordcountoutput")

    // Output configuration.
    // Let BigQueery auto-detect output schema (set to null below).
    BigQueryOutputConfiguration.configure(conf,
      outputTableId,
      null,
      outputGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      classOf[TextOutputFormat[_, _]])

    conf.set("mapreduce.job.outputformat.class",
      classOf[IndirectBigQueryOutputFormat[_, _]].getName)

    // Truncate the table before writing output to allow multiple runs.
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
      "WRITE_TRUNCATE")


    // Helper to convert JsonObjects to (word, count) tuples.
    def convertToTuple(record: JsonObject): (String, Long) = {
      val word = record.get("word").getAsString.toLowerCase
      val count = record.get("word_count").getAsLong
      (word, count)
    }

    // Helper to convert (word, count) tuples to JsonObjects.
    def convertToJson(pair: (String, Long)): JsonObject = {
      val word = pair._1
      val count = pair._2
      val jsonObject = new JsonObject()
      jsonObject.addProperty("word", word)
      jsonObject.addProperty("word_count", count)
      jsonObject
    }

    // Load data from BigQuery.
    val tableData = sc.newAPIHadoopRDD(
      conf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])

    // Perform word count.
    val wordCounts = tableData
      .map(entry => convertToTuple(entry._2))
      .reduceByKey(_ + _)

    // Display 10 results.
    wordCounts.take(10).filter(x => x._2 > 100).foreach(l => println(l))


    wordCounts.sortBy(x => -x._2).take(10).foreach(println)



    // Write data back into a new BigQuery table.
    // IndirectBigQueryOutputFormat discards keys, so set key to null.
    wordCounts
      .map(pair => (null, convertToJson(pair)))
      .saveAsNewAPIHadoopDataset(conf)



  }


}
