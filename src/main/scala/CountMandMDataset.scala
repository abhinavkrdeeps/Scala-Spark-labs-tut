package com.abhinav.practice.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CountMandMDataset {

  def main(args: Array[String]): Unit ={
    val file_path: String = "D:\\scala-labs\\src\\resources\\mnm_dataset.csv"
    val output_file_path: String = "D:\\scala-labs\\src\\resources\\mnm_dataset_agg.csv"
    // Create a Spark Session
    val spark = SparkSession.builder.master("local").appName("CountMandMDataset").getOrCreate()
    val df = spark.read.format("CSV").option("inferSchema", value = true).option("header", value = true).load(file_path)
    val countDf = df.groupBy("State", "Color")agg(count("Count"))
    val sortedCountDf = countDf.orderBy("State", "Color")
    sortedCountDf.foreach(row=>print(row))


  }

}
