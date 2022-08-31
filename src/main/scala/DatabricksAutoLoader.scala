package com.abhinav.practice.scala

import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object DatabricksAutoLoader {

  def runThisForEachBatch(df: DataFrame, batchId: Int): DataFrame={
    return df
  }

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder.master("local").appName("DatabricksAutoLoader_Example").getOrCreate()
    val schema = StructType(
      Array(
        StructField(name="caller_id",IntegerType),
        StructField(name="callee_id",IntegerType),
        StructField(name="duration",IntegerType))
    )
    // cloudFiles.useNotifications
    // when this options is set to true, the spark stream will not read the directory to find out the new files (That's slower btw).
    // Auto Loader can automatically set up a notification service and queue service that subscribe to file events from the input directory.
    // File notification mode is more performant and scalable for large input directories or a high volume of files but requires
    // additional cloud permissions for set up.
    // In Aws set up It uses SNS (Subscription service) and SQS (Queue Service to put the new files in from where the stream will read).

    // As files are discovered, their metadata is persisted in a scalable key-value store (RocksDB) in the checkpoint location
    // of your Auto Loader pipeline.
    //In case of failures, Auto Loader can resume from where it left off by information stored in the checkpoint location and
    // continue to provide exactly-once guarantees when writing data into Delta Lake.

    val df = spark
      .readStream.
      format("cloudFiles")
      .option("cloudFiles.format", "CSV")
      .option("cloudFiles.inferColumnTypes", "true") // Infer schema
      //.option("schemaEvolutionMode", "rescue")  // do not fail the job when a new column has been added. New Columns will be added as a Json in _rescue_data column
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schema") // Metastore path for holding schema changes
      .option("schemaEvolutionMode", "addNewColumns")
      .option("cloudFiles.useNotifications", true)  // Use Notification Service or Directory Listing
      .load("dbfs:/FileStore/autoloader_files_test")

    // Merge or upsert using forEachBatch
    // df.writeStream.foreachBatch(runThisForEachBatch(df,1))
    val save_path = "dbfs:/FileStore/autoloader_files_output"
    df
      .writeStream
      .format("CSV")
      .option("checkpointLocation", "dbfs:/FileStore/autoloader_files_output")
      .trigger(Trigger.Once)
      .start(save_path)

    /// Schema Inference options
    // On FIrst Read, fetch the schema and store it in a schema Metastore
    // cloudFiles.inferColumnTypes=true
    // cloudFiles.schemaHints="ID, long" -> Id should be long (Fail if found otherwise)

    // Schema Evolution
    // What to do when schema changes.
    // cloudFiles.schemaEvolutionMode
    // -- addNewColumns - (Fail the Job, update the schema Metastore. From Next Run, the new schema will be considered).
    // -- failOnNewColumns (Fail the job, donot update the schema in the metastore)
    // rescue (Do not fail the job, put everything unexpected in a _rescued_data columns)
    // ignore




  }

}
