package com.abhinav.practice.scala
// https://github.com/databricks/LearningSparkV2/tree/master/chapter3
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SFFireDataAnalysis {

  def main(args: Array[String]): Unit ={
    val file_path = "D:\\scala-labs\\src\\resources\\sf-fire-calls.csv"
    val spark = SparkSession.builder.master("local[1]").appName("SFFireDataAnalysis").getOrCreate()
    val sf_data = spark.read.format("CSV").option("inferSchema", true).
      option("samplingRatio", 0.001).option("header", true).load(file_path)
    sf_data.show(5, false)
    // Distinct call types recorded
    val distinctCallTypesCount = sf_data.select("CallType").distinct().count()
    println("distinctCallTypesCount: "+distinctCallTypesCount)
    val countDistinctNotNullTypes = sf_data.select("CallType").where(col("CallType").isNotNull).agg(countDistinct("CallType")).alias("countDistinctNotNullTypes")
    countDistinctNotNullTypes.show()
    // Find the distinct CallTypes
    val distinctCallTypes = sf_data.select("CallType").where(col("CallType").isNotNull).distinct().alias("distinctCallTypes")
    distinctCallTypes.show(false)

    sf_data.select("CallDate", "WatchDate", "AvailableDtTm").show(10, false)
    // Working With date and timestamp
    // CallDate, WatchDate,AvailableDtTm
    val sf_data_ts_modified = sf_data.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .withColumn("OnWatchDate", to_date(col("WatchDate"), "MM/dd/yyyy"))
      .withColumn("AvailableDtTs", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy HH:mm:ss a"))
      .drop("CallDate","WatchDate","AvailableDtTm")

    sf_data_ts_modified.select("IncidentDate", "OnWatchDate", "AvailableDtTs", "Zipcode").show(10, false)

    // How many calls where logged in the last seven days.
    // max(IncidentDate)-7 days
    val maxIncidentDate = sf_data_ts_modified.agg(max("IncidentDate")).as("maxIncidentDate").collect()
    var max_incident_date:java.sql.Timestamp = null
    for(row:Row <- maxIncidentDate){
      max_incident_date = row.getAs[java.sql.Timestamp]("max(IncidentDate)")
    }
    println("max_incident_date: "+max_incident_date)
    val callsLoggedInLastSevenDays = sf_data_ts_modified.where(s"IncidentDate >= cast(date_sub(cast('${max_incident_date}' as date), 7) as timestamp) and IncidentDate< '${max_incident_date}'")
    // callsLoggedInLastSevenDays.orderBy(col("IncidentDate").desc).show(false)

    // Get distinct date for last 7 days
    callsLoggedInLastSevenDays.select("IncidentDate").distinct().orderBy(col("IncidentDate").desc).show(false)

    // Get All distinct years in which incidents were reported
    val incidentYearsDF =  sf_data_ts_modified.select(year(col("IncidentDate")).alias("IncidentYear"))
    println("Distinct Incident Years")
    incidentYearsDF.orderBy(col("IncidentYear").desc).show(false)


    //////////////////////  Aggregation //////////////////////
    // 1) Most common types of fire calls
    val fireCallTypeWithCountDf = sf_data_ts_modified.groupBy("CallType").agg(count("CallNumber")).alias("CallTypeCount")
    fireCallTypeWithCountDf.select("CallType", "count(CallNumber)").orderBy(col("count(CallNumber)").desc).show(false)

    // 2) What zip codes accounted for most calls.
    // Get Row with max Count
    val zipCodeReportedCountDF = sf_data_ts_modified.groupBy("ZipCode").agg(count(col("CallNumber"))).alias("callCountZipped")
    zipCodeReportedCountDF.select("ZipCode", "count(CallNumber)").orderBy(col("count(CallNumber)").desc).show(false)


    // Statistical Analysis
    // sum of numOfAlarms, avg delay, max and min delay time
    val statsDf = sf_data_ts_modified.agg(sum("NumAlarms"), avg("Delay"), max(col("Delay")), min(col("Delay")))
    statsDf.show(false)

    // Analytical Questions
    // 1) what were all the different type of fire calls in 2018.
    val fireCallsType = sf_data_ts_modified.where("year(IncidentDate)=2018").select("CallType", "IncidentDate").distinct()
    println("1) what were all the different type of fire calls in 2018.")
    fireCallsType.show(false)

    // 2) Which months within the year 2018 saw the highest number of fire calls.
    var monthWiseCall = sf_data_ts_modified.withColumn("month", month(col("IncidentDate")))
    monthWiseCall = monthWiseCall.where("year(IncidentDate)=2018").groupBy("month").agg(count(col("CallNumber")))

    println("2) Which months within the year 2018 saw the highest number of fire calls.")
    var maxMonthWiseCall = monthWiseCall.select("count(CallNumber)").agg(max(col("count(CallNumber)"))).collect()
    var resultingMonthWithMaxFireCalls:Long = 0
    for(month <- maxMonthWiseCall){
      resultingMonthWithMaxFireCalls = month.getAs[Long]("max(count(CallNumber))")
    }
    println("resultingMonthWithMaxFireCalls: "+resultingMonthWithMaxFireCalls)


    // 3) Which has Worst Response Time
    // Worst Response Time = Maximum Delay Done
    val maxDelayDF = sf_data_ts_modified.agg(max("Delay")).collect()
    var maxDelay:Double = 0.0
    for(row <- maxDelayDF){
      maxDelay = row.getAs[Double]("max(Delay)")
    }
    println("maxDelay: "+maxDelay)
    sf_data_ts_modified.select("CallType", "Address").where(s"Delay=${maxDelay}").show(false)































  }

}
