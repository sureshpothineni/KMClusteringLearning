package com.fpl.spark

import java.io.BufferedWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.databricks.spark.csv.CsvContext
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.file.Paths
import java.nio.charset.StandardCharsets
import java.nio.file.Files

object ReportGenerator {
  
  def main (args:Array[String]){
    val conf = new SparkConf().setAppName("Report executor").setMaster("local")
    val contxt = new SparkContext(conf)
    val sqlContxt = new SQLContext(contxt)
    val csvContxt = new CsvContext(sqlContxt)
  //  val outf = new BufferedWriter( new OutputStreamWriter( new FileOutputStream("C:/KVS/testopt")));
     val buffer = new StringBuffer();  
  val fileDF = sqlContxt.read.option("charset", "UTF-8").text("C:/spark/data/Tamil.txt")  // load("C:/KVS/test_non_eng1.txt", Map("charset"-> "UTF-16")) //read.option("charset", "UTF-16").text("C:/KVS/test_non_eng.txt")
  val wdRDD = fileDF.map(x =>x.getString(0)).flatMap( x => x.split(" ") ).map(x =>(x,1)).reduceByKey((x,y)=>x+y)
  wdRDD.collect().foreach(x => printPair(x,buffer))
  
 /* //fileDF.collect.map {x => val input = x.getString(0)
                        // print(input)
    //Files.write(Paths.get("file.txt"), "file contents".getBytes(StandardCharsets.UTF_8))
    //outf.write(input)
    //buffer.append(input)
                 for( y <- input.getBytes(StandardCharsets.UTF_8)){
                 // val z = y.toInt
                   println("%x".format(y))
                   //print(String.format("%x", y))
                 }
                 
    }*/
  Files.write(Paths.get("C:/KVS/testoptT"),buffer.toString().getBytes(StandardCharsets.UTF_8))
    //outf.close()
 /*   val fileDF = sqlContxt.csvFile("C:/spark/data/spkTotOpt",false).toDF("DeviceName","RegionCd","EventEndTime","EventStartTime","diff")
   
   fileDF.select("DeviceName","EventStartTime",fileDF("diff").cast(to:Int))*/
    
  }
  
  def printPair(tuple:(String,Int),buffer:StringBuffer){
    
    buffer.append(tuple._1 +" => " +tuple._2).append(" \n")
  }
}