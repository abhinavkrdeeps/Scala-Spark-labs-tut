package com.abhinav.practice.scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataBank {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder().master("local[2]").appName("DataBank").getOrCreate()
    println(args)
    val regionsFilePath: String = args(0)
    val customerNodesFilePath: String = args(1)
    val customerTransactionsFilePath: String = args(2)


    // Regions schema
    val regionSchema: StructType = StructType(Array(
      StructField(name="region_id", dataType = IntegerType),
      StructField(name="region_name", dataType = StringType)
    ))

    // customer_nodes schema
    val customerNodeSchema: StructType = StructType(Array(
      StructField(name="customer_id", dataType = IntegerType),
      StructField(name="region_id", dataType = IntegerType),
      StructField(name="node_id", dataType = IntegerType),
      StructField(name="start_date", dataType = StringType),
      StructField(name="end_date", dataType = StringType)
    ))

    // customer_transactions schema
    val customerTransactionsSchema: StructType = StructType(Array(
      StructField(name="customer_id", dataType = IntegerType),
      StructField(name="txn_date", dataType = StringType),
      StructField(name="txn_type", dataType = StringType),
      StructField(name="txn_amount", dataType = IntegerType)
    ))

    // regions Df
    val regionsDF = spark.read.format("CSV").schema(regionSchema).option("ignoreTrailingWhiteSpace",value = true).option("header",value = true).load(regionsFilePath)

    // customer_nodes Df
    val customerNodesDF = spark.read.format("CSV").schema(customerNodeSchema).option("ignoreTrailingWhiteSpace",value = true)
      .option("ignoreLeadingWhiteSpace",value = true).option("header",value = true).load(customerNodesFilePath)

    //customer_transactions DF
    val customerTransactionsDF = spark.read.format("CSV").schema(customerTransactionsSchema).option("ignoreTrailingWhiteSpace",value = true)
      .option("ignoreLeadingWhiteSpace",value = true).option("header",value = true).load(customerTransactionsFilePath)

    regionsDF.show()

    customerNodesDF.show(5)

    customerTransactionsDF.show(5)

    customerNodesExplanation(regionsDF, customerNodesDF, customerTransactionsDF)

    customersTransactionsExplanation(regionsDF, customerNodesDF, customerTransactionsDF)
  }

  def customersTransactionsExplanation(regionsDF: DataFrame, customerNodesDF: DataFrame, customerTransactionsDF: DataFrame): Unit ={

    // What is the unique count and total amount for each transaction type?
    val uniqueCountAndTotalAmountPerTransaction = customerTransactionsDF.groupBy("txn_type").agg(
      countDistinct("txn_type").alias("unique_count"),
      sum(col("txn_amount").alias("total_amount_per_transaction")))
    println(s"unique count and total amount for each transaction type: ${uniqueCountAndTotalAmountPerTransaction.show()}")

    // What is the average total historical deposit counts and amounts for all customers?
    val depositCountsAndAmounts = customerTransactionsDF.filter("txn_type='deposit' ").agg(
      count("customer_id").alias("deposit_customer_count"),sum("txn_amount").alias("total_deposit_txn_amount") )
    println(s"average total historical deposit counts and amounts for all customers: ${depositCountsAndAmounts.show()}")

    // For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
    var extractMonthYrDF = customerTransactionsDF
      .withColumn("transaction_month", month(col("txn_date").cast("date")))
      .withColumn("transaction_year", year(col("txn_date").cast("date")))
    extractMonthYrDF = extractMonthYrDF.select("customer_id","txn_date","txn_type", "transaction_month","transaction_year")

    var tempGroupedTransactionDf = extractMonthYrDF.groupBy("customer_id","txn_type", "transaction_month", "transaction_year")
      .agg(count("txn_type").alias("num_txn")).select("customer_id","txn_type", "transaction_month", "transaction_year","num_txn")

    val filteredCustomersDF = tempGroupedTransactionDf.where("(txn_type='deposit' and num_txn>1) " +
      "or (txn_type='purchase' and num_txn=1)" +
      "or (txn_type='withdrawal' and num_txn=1) ").select("*")

    filteredCustomersDF.show()

    println("Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month: ")
    filteredCustomersDF.agg(countDistinct("customer_id")).show()

    // What is the closing balance for each customer at the end of the month?




  }

  def customerNodesExplanation(regionsDF: DataFrame, customerNodesDF: DataFrame, customerTransactionsDF: DataFrame): Unit ={
   //  How many unique nodes are there on the Data Bank system?
    val uniqueNodeCount = customerNodesDF.select("node_id").distinct().count()
    println(s"unique nodes on the Data Bank system: $uniqueNodeCount")

    // What is the number of nodes per region?
    val numNodesPerRegion = customerNodesDF.groupBy("region_id").agg(count("node_id").alias("num_nodes_per_region_"))
    println(s" number of nodes per region\n: ${numNodesPerRegion.show()}")

    // How many customers are allocated to each region?
    val customersAllocatedPerRegion = customerNodesDF.groupBy("region_id").agg(countDistinct("customer_id").alias("num_customers_per_region")).orderBy("region_id")
    println(s"customers are allocated to each region\n: ${customersAllocatedPerRegion.show()}")

    // How many days on average are customers reallocated to a different node?
    val customerAllocatedTempDF = customerNodesDF.filter("end_date!='9999-12-31' ").groupBy("customer_id","node_id","start_date", "end_date").agg(col("customer_id"),
      col("node_id"), datediff(col("end_date").cast(DateType),col("start_date").cast(DateType)).alias("num_days")).orderBy("customer_id","node_id")

    customerAllocatedTempDF.show()

    val sumDiff = customerAllocatedTempDF.groupBy("customer_id", "node_id").agg(col("customer_id"),col("node_id"),
      sum("num_days").alias("sum_days"))

    val averageDaysCustomersAllocatedToDiffNode = sumDiff.agg(round(avg("sum_days"), 2).alias("avg_reallocation_days"))
    averageDaysCustomersAllocatedToDiffNode.show()

    // What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
    println(averageDaysCustomersAllocatedToDiffNode.stat.approxQuantile("avg_reallocation_days", Array(0.8, 0.95), 0.1).mkString("Array(", ", ", ")"))
  }

}
