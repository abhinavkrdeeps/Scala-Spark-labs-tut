package com.abhinav.practice.scala
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object SchemaExamples {

  def runThis(df: DataFrame): Unit ={

  }

  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder.master("local").appName("StructuredDFApisExamples").getOrCreate()
    val jsonFilePath = "D:\\scala-labs\\src\\resources\\blogs.json"
    val schema = StructType(Array(
      StructField(name = "Id", IntegerType),
      StructField(name = "First",dataType = StringType),
      StructField(name = "Last", StringType),
      StructField(name = "Url", StringType),
      StructField(name = "Published", StringType),
      StructField(name = "Hits", IntegerType),
      StructField(name = "Campaigns", dataType= ArrayType(StringType))
    ))
    val df = spark.read.format("JSON").schema(schema).load(jsonFilePath)
    df.show(false)
    // Stop the underlying spark context
    spark.stop()



  }

}
