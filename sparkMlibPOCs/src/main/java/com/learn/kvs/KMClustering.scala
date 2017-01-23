package com.learn.kvs

import java.util.ArrayList

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext

import com.databricks.spark.csv.CsvContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Level
import org.apache.log4j.Logger


//scala.collection.mutable.ArrayBuffer



object Clustering {
	def main(args: Array[String]){

		val conf = new SparkConf().setAppName("Spark K-means clustering ").setMaster("local")
				val contxt = new SparkContext(conf)
				val sqlContxt = new SQLContext(contxt)
				val csvContxt = new CsvContext(sqlContxt)
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
				val fileDF = sqlContxt.csvFile("C:/Users/sxk0hg8/workspace/scala/sparkMlibPOCs/src/main/resources/Bids_wins_loss_cor.csv",true)
				val inputs = Source.fromFile("C:/Users/sxk0hg8/workspace/scala/sparkMlibPOCs/src/main/resources/cluster_input_main.txt").getLines()
				for(input <-inputs){
				  val specs = input.split("#")
				  require(specs.length > 3,s"Invalid parameter specifications, required: columnsFilePath,ClustersLimit and IterationCount, [comments], found ${specs.length}")
					val columns = Source.fromFile(specs(0)).getLines().take(1).next().split(",")
					
							import sqlContxt.implicits._
							val filterRDD = fileDF.select(columns.head,columns.tail:_*)
							filterRDD.select("cast(Customer_Classification as Double) Customer_Classification").show(10)
							val rowId = columns(0)
							val lableObj = filterRDD.map{ row => val arrayL = new ArrayBuffer[Double]()
							var keyVal:String = null
							for(column <- columns){
								if(keyVal == null){
									keyVal =row.getAs[String](column)
								}else{
									arrayL +=(row.getAs[String](column).toDouble)
								}
							}
							//  val recId = arrayL.remove(0)
									val array = arrayL.toArray 
									(keyVal, Vectors.dense(array.head,array.tail:_*))

					}
					lableObj.cache()  // this statement is required to improve the performance.

					// Trains a k-means model
					for(itr <- 2 to specs(1).toInt){
					val model = KMeans.train(lableObj.map(_._2),itr,specs(2).toInt,12345) //seed 12345 to keep results consistent
					val wsse = model.computeCost(lableObj.map(_._2))
					//model.clusterCenters.foreach(println)

					val kmeanDF = lableObj.map{x => val custId = model.predict(x._2)
					(x._1,custId)
					}.toDF(s"${rowId}","ClustId")

					val totDF = fileDF.join(kmeanDF, s"${rowId}")
					var strBuffer = new StringBuffer()
          model.clusterCenters.foreach(x =>strBuffer.append(x.toArray.mkString(",")).append("/n"))
					// totDF.filter("ClustId = 0")show()
          val optFile = s"${specs(0)}with${itr}Clusters"
					totDF.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(optFile)
          scala.tools.nsc.io.File(optFile+".csv").writeAll(strBuffer.toString())
					model.clusterCenters.foreach(println)
					lableObj.unpersist()
					}
				}

		//val kmeans =   new KMeans().setK(3).setFeaturesCol("features") .setPredictionCol("prediction").setMaxIter(50)
		// val model = kmeans.fit(lableDF)
		//model.clusterCenters.foreach(println)
		//val train = model.
	}

}