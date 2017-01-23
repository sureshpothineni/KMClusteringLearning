package com.fpl.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SQLContext

object StreamingEx {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[4]")
   // val contxt = new SparkContext(conf)
    val scontxt = new StreamingContext(conf,Seconds(15))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092", ConsumerConfig.GROUP_ID_CONFIG ->"test_now",
                      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->"org.apache.kafka.common.serialization.StringDeserializer",
                      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->"org.apache.kafka.common.serialization.StringDeserializer",
                      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true", "spark.kafka.poll.time" ->"5000")
    val topics = "kvsKafka1,test".split(",").toSet
      scontxt.checkpoint("C:/tmp")
    
    val kafkaRdds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scontxt, kafkaParams, topics)
    val lines = kafkaRdds.map(_._2)
    lines.foreachRDD{rdd =>
          if (rdd.isEmpty) {
            println("no messages received")
          }else{
            println("count received " + rdd.count)
          }
          val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
           import sqlContext.implicits._
        import org.apache.spark.sql.functions._

        val cdrDF = rdd.toDF()
        // Display the top 20 rows of DataFrame
        println("CDR data")
        cdrDF.show()
          
    }
   
    //val mesgs = kafkaRdds.foreachRDD (rdd => rdd.map(_._2).foreach(println))
    scontxt.start()
    scontxt.awaitTermination()
    scontxt.stop(true,true)
     
  }
}