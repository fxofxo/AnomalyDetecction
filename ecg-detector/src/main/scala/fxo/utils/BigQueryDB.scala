package fxo.utils

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/*
Configure a connection for input output writes to 2 bigquery databases,
if table Id are null ignore the corresponding configurations

 */

object BigQueryDB {

  var c = 3
  private def addProjectId( conf : Configuration, inStr : String): Unit = {

  }

  /* input or output data base ID could be external bigquery DB, with full qualified ID
    as "publicdata:samples.shakespeare"
    or internal (to the project) DB in the form  "datasetname:tableName" in this case
    projectId will be added to form full qualified ID
   */
  def createConnection(sc: org.apache.spark.SparkContext,
                       inputTableId: String,
                       outputTableId: String ): Configuration = {




    val conf = sc.hadoopConfiguration  // Input configuration.
    val projectId = conf.get("fs.gs.project.id")
    val bucketId = conf.get("fs.gs.system.bucket")

    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucketId)

    if( inputTableId != null) {
      var fullInputTableId : String = inputTableId
      if (inputTableId.startsWith(":") ) {
        fullInputTableId = projectId  + inputTableId
        }

      BigQueryConfiguration.configureBigQueryInput(conf, fullInputTableId)
    }

    // Output parameters.

    // Temp output bucket that is deleted upon completion of job.
    val outputGcsPath = "gs://" + bucketId + "/hadoop/tmp/bigquery/" + "tableId"

    // Output configuration.
    // Let BigQuery auto-detect output schema (set to null below).
    if( outputTableId != null) {
      var fullOutputTableId =  outputTableId
      if (outputTableId.startsWith(":")) {
        fullOutputTableId = projectId + ":" + outputTableId
      }

      BigQueryOutputConfiguration.configure(conf,
                                            outputTableId,
                                            null,
                                            outputGcsPath,
                                            BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
                                            classOf[TextOutputFormat[_, _]])


      conf.set("mapreduce.job.outputformat.class",
              classOf[IndirectBigQueryOutputFormat[_, _]].getName)
              // Truncate the table before writing output to allow multiple runs.
             // conf.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, "WRITE_TRUNCATE")
    }
    conf
  }

  // Write data into a new BigQuery table.
  // IndirectBigQueryOutputFormat discards keys, so set key to null.

}


