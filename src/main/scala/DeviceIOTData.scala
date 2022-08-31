package com.abhinav.practice.scala

import org.apache.spark.sql._

case class DeviceIOTData(device_id:String, device_name:String, ip:String,cca2:String,cca3:String,cn:String,latitude:Double, longitude:Double,
                         scale:String,temp:Long,humidity:Long,battery_level:Long,c02_level:Long,lcd:String,timestamp:Long)

object IotDataAnalysis{

  def main(args:Array[String]): Unit ={
    val file_path = "D:\\scala-labs\\src\\resources\\iot_devices.json"
    val spark = SparkSession.builder.master("local").appName("IotDataAnalysis").getOrCreate()
    import spark.implicits._
    val df = spark.read.json(file_path).as[DeviceIOTData]
    df.show(truncate = false)

  }
}
