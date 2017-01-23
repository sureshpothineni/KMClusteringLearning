';/*
 * CTS - Technathon Event - DataGators Team - Classification Model 
 */

package com.learn.kvs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger


object RfpClassification {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RfpClassification")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
   
    // Load and parse the training data file.
    val rfpTrainingData = MLUtils.loadLibSVMFile(sc, args(0))
    
    // Split the data into training and test sets (30% held out for testing)
    val splits = rfpTrainingData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val rfpClassificationModel = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val rfpPredict = testData.map { point =>
      val prediction = rfpClassificationModel.predict(point.features)
      (point.label, prediction)
    }
    //val predictionAndLabel = prediction zip(testData map(_ label))
    
    val predictionError = rfpPredict.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Validation Data : Prediction Error = " + predictionError)
    println("Classification Model - Decision Tree \n" + rfpClassificationModel.toDebugString)
    
    // Load and parse the predict data file.
    val predictData = MLUtils.loadLibSVMFile(sc, args(1))
    
    // Predict Win/Loss for new RFPs
    val rfpPredictWL = predictData.map { point =>
      val prediction = rfpClassificationModel.predict(point.features)
      (prediction)
     }
     println("Prediction output for new RFPs:")
     rfpPredictWL.collect().foreach(println);

    // Save and load model
   // model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
  //  val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    
  }
}