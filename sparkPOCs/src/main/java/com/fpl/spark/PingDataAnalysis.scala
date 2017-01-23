package com.fpl.spark

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.joda.time.format.DateTimeFormat

import com.databricks.spark.csv.CsvContext
import scala.collection.mutable.ArrayBuffer


case class PingRecord(srcName:String,startTime:String,stTimeSec:Long, ping1:String,ping2:String,ping3:String, var endTime:String)


object PingDataAnalysis {
  
	def main(args:Array[String]){
		val conf = new SparkConf().setAppName("EventsBarChart").setMaster("local[*]")
				.setSparkHome("file:///C:/spark/").set("spark.local.dir", "/tmp/spark-temp")
				.set("spark.worker.cleanup.enabled", "true")
		val contxt = new SparkContext(conf)
		val sqlContxt = new SQLContext(contxt)
		val csvcontxt = new CsvContext(sqlContxt)
		
		// Reading csv file
		val fileDF = sqlContxt.csvFile("C:/spark/data/ping_extract_all.csv", true).cache()
		val dfTouple= fileDF.map { row =>(row.getAs[String]("ami_dvc_name"),toPingRecord(row)) }
    //fileDF.
		val procRDD = dfTouple.groupByKey().map{
		                  	tple => val listVar = tple._2.toList
              					val sortList = listVar.sortBy(_.startTime)
              					var startRec:PingRecord =null
              					var arrBuffer = new ArrayBuffer[PingRecord]()

              					for(itrRec<-sortList){
              								if(itrRec.ping1.equals("f") || itrRec.ping2.equals("f") ||
              										(!itrRec.ping3.isEmpty() && itrRec.ping3.equals("f"))){

              										if(startRec == null || ((itrRec.stTimeSec - startRec.stTimeSec) < 60*60000L)){
              												startRec = itrRec
              											}else {
              											    startRec.endTime = itrRec.startTime
              											    arrBuffer += startRec
              											    startRec = null
              											}
              								}
              					}
		                  	arrBuffer.toSeq
		}
		
		import sqlContxt.implicits._
	//	val procDF = procRDD.toDF()
	val procDF = procRDD.flatMap(x => x.toList).toDF().cache()
		
		procDF.registerTempTable("PingLog") // registering temporary table.
		
		//procDF.show()
		// Query to build aggregation to find device total down time in seconds
	/*	sqlContxt.sql("""select srcName,
				sum((unix_timestamp(endTime,'yyyy-MM-dd HH:mm:ss-SS')- unix_timestamp(startTime,'yyyy-MM-dd HH:mm:ss-SS') ))/3600 as time_diff
				from PingLog 	group by srcName order by time_diff desc """
				).show()*/
		sqlContxt.sql("""select srcName,startTime,endTime from PingLog order by srcName """
				).coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("C:/spark/data/pingOpt")
		
	}
	
  // Function to build ping records from input data
	def toPingRecord(start:Row):PingRecord ={
			val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss-SS");
			val srcName = start.getAs[String]("ami_dvc_name")
			val startTime = start.getAs[String]("srcfile_btch_tmstmp")
			val ping1 = start.getAs[String]("packet_lost_flag_ping1")
			val ping2 = start.getAs[String]("packet_lost_flag_ping2")
			val ping3 = start.getAs[String]("packet_lost_flag_ping3")
			val startSec = formatter.parseMillis(startTime) 
			return PingRecord(srcName,startTime,startSec,ping1,ping2,ping3,null)

	}

}