package com.abhinav.practice.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object ColumnsAndExpressions {

  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder.master("local").appName("ColumnsAndExpressions").getOrCreate()
    val jsonFilePath = "D:\\scala-labs\\src\\resources\\blogs.json"
    // create a dataframe from blogs.json
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
    // Access A Particular column
    print(df.col("Id"))
    // use expr to compute a value
    println("Hits * 2")
    df.select(expr("Hits *2")).show(false)

    // user ColumnrType
    println("Using ColumnType")
    df.select(col("Hits") * 2).show(false)

    // Find BigHitters
    println("Find BigHitters")
    df.withColumn("Is_BigHitters", expr("Hits > 10000")).show(false)

    // using Column Type
    println("Find BigHitters ColumnType")
    df.withColumn("Is_BigHitters", col("Hits").geq(10000)).show(false)

    // create a new column (Author_Id) by concatenating (First,Last and Id)
    println("Find Author Id")
    df.withColumn("Author_ID", concat(col("First"), lit("_"), col("Last"), lit("_"), col("Id"))).show(false)

    // sort in ascending order of hits
    println("Sort")
    df.sort(col("Hits").desc).show(false)
  }

}
