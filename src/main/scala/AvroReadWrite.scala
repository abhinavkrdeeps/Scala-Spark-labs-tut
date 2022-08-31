package com.abhinav.practice.scala

import org.apache.spark.sql.SparkSession

object AvroReadWrite {

  //

  def main(args: Array[String]): Unit ={
    val filePath: String = "D:\\big-data-file-formats\\avro\\users.avro"
    val spark = SparkSession.builder().master("local[2]").appName("AvroReadWrite").getOrCreate()
    val df = spark.read.format("avro").load(filePath)
    df.show()
  }

}
