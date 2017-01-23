package com.learn.kvs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object LinearRegression {
  
  def main(args:Array[String]){
    
    val conf = new SparkConf().setAppName("Linear Regression").setMaster("local")
    val contxt = new SparkContext(conf)
    val fileRDD = contxt.textFile("C:/spark/data/linearRegression.csv",3).cache()
    val vctrRDD = fileRDD.map { x => val arrVal =x.split(",") 
                                val vctr = Vectors.dense( arrVal(1).toDouble,arrVal(2).toInt,arrVal(3).toInt,arrVal(4).toInt,
                                arrVal(5).toInt,arrVal(6).toInt,arrVal(7).toInt,arrVal(8).toInt,arrVal(9).toInt,arrVal(10).toDouble,
                                arrVal(11).toInt,arrVal(12).toDouble)
                                LabeledPoint(arrVal(0).toDouble,vctr)
                              }
    val splits = vctrRDD.randomSplit(Array(0.8,0.2))
    val train = splits(0).cache()
    val test = splits(1).cache()
    
    val alogrithm = new LinearRegressionWithSGD()
    val model = alogrithm.run(train)
    val predict = model.predict(test.map(_.features))
    val compare = predict.zip(test.map(_.label))
    compare.foreach(x => println(s"Actual:${x._2},Predict: ${x._1}"))
    contxt.stop
    
  }

}