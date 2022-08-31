package com.abhinav.practice.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
// CallNumber,UnitID,IncidentNumber,CallType,CallDate,WatchDate,CallFinalDisposition,AvailableDtTm,Address,City,
// Zipcode,Battalion,StationArea,Box,OriginalPriority,Priority,FinalPriority,ALSUnit,CallTypeGroup,NumAlarms,
// UnitType,UnitSequenceInCallDispatch,FirePreventionDistrict,SupervisorDistrict,Neighborhood,Location,RowID,Delay
object FileStreaming {

  def main(args: Array[String]): Unit ={
    // awsuser Monu@123456
    val spark = SparkSession.builder().master("local[2]").appName("FileStreaming").getOrCreate()
    val schema = StructType(Array(
      StructField(name = "State", dataType =StringType),
      StructField(name = "Color", dataType =StringType),
      StructField(name = "Count", dataType =StringType)
    ))
    val fileStreamingDF = spark.readStream.option("header",true).schema(schema).csv("D:\\scala-labs\\src\\file_streaming");
    val processedDf = fileStreamingDF.groupBy("State", "Color").agg(count("Count").as("agg_count"))
    val streamingQuery = processedDf
      .writeStream
      .format("CSV")
      .outputMode("append") /* append (Only New Rows were inserted. This can be used when we know that the streaming query will
      // not change the already existing table rows),

       complete
       In Complete Mode, Entire updated result will be written to the sink.

       update
       In Update Mode, only the rows that were updated since the last trigger were  written to the sink.

      */
      .trigger(Trigger.ProcessingTime("60 second")) // When do we want spark to read new rows in streaming buffer.
      .option("checkPointLocation","")  // Hdfs compatible filesystem where a streaming query saves its progress about how many re
      // records have been processed. Upon failure, this metadata is used to pick up the point from where it will re-execute.
      .start("D:\\scala-labs\\src\\streaming_output\\agg.csv") // aysnc call

    // start() is a non-blocking call. So, it will return as soon as the query starts.

    streamingQuery.awaitTermination() //  block the main thread to wait until the streaming query completes.

  }

}
