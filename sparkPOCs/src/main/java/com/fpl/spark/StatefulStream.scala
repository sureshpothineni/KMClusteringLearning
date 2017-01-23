package com.fpl.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import kafka.serializer.StringDecoder

object StatefulStream {
  
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Stateful Streaming").setMaster("local[*]")
    val scontxt = new StreamingContext(conf,Seconds(5))
    scontxt.checkpoint("C:/temp")
     val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val topics = "kvsKafka".split(",").toSet
    
    val kafkaRdd = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](scontxt,kafkaParams,topics)
    val streamData = kafkaRdd.map(_._2)
    streamData.map(println)
    
    
  }
}