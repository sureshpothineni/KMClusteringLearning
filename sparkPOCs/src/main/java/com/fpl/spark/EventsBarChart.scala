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


case class EventRecord(srcName:String,eventId:Int,startTime:String,stInSec:Long,srcUtilId:String,primiseId:String,regionCd:String, var endTime:String)


object EventsBarChart {
	def main(args:Array[String]){
		val conf = new SparkConf().setAppName("EventsBarChart").setMaster("local[*]")
				.setSparkHome("file:///C:/spark/").set("spark.local.dir", "/tmp/spark-temp")
				.set("spark.worker.cleanup.enabled", "true")
		val contxt = new SparkContext(conf)
		val sqlContxt = new SQLContext(contxt)
		val csvcontxt = new CsvContext(sqlContxt)
		
		// Reading csv file
		val fileDF = sqlContxt.csvFile("C:/spark/data/dvc_events.csv", true).cache()
		val dfTouple= fileDF.map { row =>(row.getAs[String]("SRC_NAME"),toEventRecord(row)) }

		val procRDD = dfTouple.groupByKey().map{
		                  	tple => val listVar = tple._2.toList
              					val sortList = listVar.distinct.sortBy(_.stInSec)
				              	val startList = sortList.filter { x => x.eventId == 12010 }
			                  val endList = sortList.filter { x => x.eventId == 12012 }
			           // println(s" ${tple._1} lists sizes ${sList.size},${stList.size},${edList.size}")

			                  for(stRec<-startList) yield fillEndTime(stRec,endList)
		}
		import sqlContxt.implicits._
		val procDF = procRDD.flatMap(x => x.toList).toDF().cache()
		
		procDF.registerTempTable("EventLog") // registering temporary table.
			val startbound ="'10-06-16 00:00:00'"
			val upperbound ="'10-14-16 00:00:00'"
		//procDF.select("srcName", "regionCd").filter(unix_timestamp(procDF("startTime"),"'dd-MMM-yy h.mm.ss.SSSSSSSSS aa'")).
			
		// Query to build report with device level event details 
		//sqlContxt.sql(s"""select max(endTime), min(startTime) from EventLog """).coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("C:/spark/data/spkMMOpt")
		
		// Query to build report with device level event details 
		sqlContxt.sql(s"""select srcName, regionCd, startTime,endTime,
				(unix_timestamp(endTime,'dd-MMM-yy hh.mm.ss.SSSSSSSSS aa')-unix_timestamp(startTime,'dd-MMM-yy hh.mm.ss.SSSSSSSSS aa') )as time_diff
				from EventLog where unix_timestamp(startTime,'dd-MMM-yy h.mm.ss.SSSSSSSSS aa') between 
				unix_timestamp(${startbound},'MM-dd-yy hh:mm:ss')
				and unix_timestamp(${upperbound},'MM-dd-yy hh:mm:ss')
				order by  srcName """).coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("C:/spark/data/spkTotOpt1")
		
			// Query to build aggregation to find device total down time in seconds
		/*sqlContxt.sql(s"""select srcName,
				sum((unix_timestamp(endTime,'dd-MMM-yy hh.mm.ss.SSSSSSSSS aa')-unix_timestamp(startTime,'dd-MMM-yy hh.mm.ss.SSSSSSSSS aa') ))/60 as time_diff
				from EventLog where unix_timestamp(startTime,'dd-MMM-yy h.mm.ss.SSSSSSSSS aa') between 
				unix_timestamp(${startbound},'MM-dd-yy hh:mm:ss')
				and unix_timestamp(${upperbound},'MM-dd-yy hh:mm:ss')
				group by srcName order by time_diff desc """
				).coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("C:/spark/data/spkDivOpt")
		*/
		 // Query to build aggregation to find average down time in seconds for region 
			
	/*	sqlContxt.sql(s"""select regionCd,
				avg((unix_timestamp(endTime,'dd-MMM-yy hh.mm.ss.SSSSSSSSS aa')-unix_timestamp(startTime,'dd-MMM-yy hh.mm.ss.SSSSSSSSS aa') ))as time_diff
				from EventLog where unix_timestamp(startTime,'dd-MMM-yy h.mm.ss.SSSSSSSSS aa') between 
				unix_timestamp(${startbound},'MM-dd-yy hh:mm:ss')
				and unix_timestamp('10-29-16 01:22:14','MM-dd-yy hh:mm:ss')
				group by regionCd order by time_diff desc """
				).coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("C:/spark/data/spkRegOpt")
*/
	}

  // Function to populate end time to the event record
	def fillEndTime(start:EventRecord, endList:List[EventRecord]):Option[EventRecord] ={
			start.endTime = start.startTime  // initializing to start time.
					var found = false
					for(obj <- endList){
						if(found == false && obj.stInSec >= start.stInSec){
							start.endTime = obj.startTime
									return Some(start)
						}
					}
			return None
	}
	
	def cleanseData(recs:List[EventRecord]):List[EventRecord]={
	  var recItr = recs.iterator
	  var unique = true
	  while(recItr.hasNext){
	    val rec = recItr.next()
	    var tItr = recItr.toIterator
	    while(tItr.hasNext && unique){
	      val temp = tItr.next()
	      if((temp.stInSec - rec.stInSec) < 60*60){
	       // recs. -= temp
	      }
	    }
	    
	  }
	
	  return recs
	}
	
  // Function to build event records from input data
	def toEventRecord(start:Row):EventRecord ={
			val formatter = DateTimeFormat.forPattern("dd-MMM-yyyy HH.mm.ss.SSSSSSSSS aa");
			val srcName = start.getAs[String]("SRC_NAME")
			val eventId = start.getAs[String]("EVENT_ID").toInt
			val startTime = start.getAs[String]("EVENT_TIME")
			val stInSec = formatter.parseMillis(startTime)/1000
			val srcUtilId = start.getAs[String]("SRC_UTIL_ID")
			val primiseId = start.getAs[String]("UTILPREMISEID")
			val regionCd = start.getAs[String]("REGION_CODE")

			return EventRecord(srcName,eventId,startTime,stInSec,srcUtilId,primiseId,regionCd,null)

	}

}