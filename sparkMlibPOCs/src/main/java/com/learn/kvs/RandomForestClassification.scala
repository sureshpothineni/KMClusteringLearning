/*
 * CTS - Technathon Event - DataGators Team - Classification Model 
 */

package com.learn.kvs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.databricks.spark.csv.`package`.CsvContext
import org.apache.spark.sql.SQLContext
import scala.io.Source
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import scala.collection.mutable.ArrayBuffer

object RandomForestClassification {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RfpClassification").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    val csvSc = new CsvContext(sqlc)
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
   
    // Load and parse the training data file.
    val fileDF = sqlc.csvFile("C:/spark/data/linearRegression.csv",true)
      val columns = Source.fromFile("C:/spark/data/linearRegColumns.txt").getLines().take(1).next().split(",")
    import sqlc.implicits._
   // val intCalms = columns.split(",").toSeq

    val filterRDD = fileDF.select(columns.head,columns.tail:_*)
    //filterRDD.show()
    
   val lableObj = filterRDD.map{ row => val arrayL = new ArrayBuffer[Double]()
                                     for(column <- columns){
                                       arrayL +=(row.getAs[String](column).toDouble)
                                     }
                                     val recId = arrayL.remove(0).toInt
                                     val array = arrayL.toArray 
                                     LabeledPoint(recId, Vectors.dense(array.head,array.tail:_*))
     /* row => val vctr = Vectors.dense(row.getString(1).toDouble,row.getString(2).toDouble,
                                       row.getString(3).toDouble,row.getString(4).toDouble,row.getString(5).toDouble,
                                       row.getString(6).toDouble,row.getString(7).toDouble,row.getString(8).toDouble,
                                       row.getString(9).toDouble,row.getString(10).toDouble,row.getString(11).toDouble,
                                       row.getString(12).toDouble)
                                       LabeledPoint(row.getString(0).toInt,vctr)*/
                            }
    lableObj.cache()  // this statement is required to improve the performance.

    
    //val rfpTrainingData = MLUtils.loadLibSVMFile(sc, args(0))
    
    // Split the data into training and test sets (30% held out for testing)
    val splits = lableObj.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "entropy" // "gini"
    val featureSubsetStrategy = "auto"
    val maxDepth = 5
    val maxBins = 32
    val numTrees = 5  // need to tune this by giving range  2 (num classes)  to 5 (max depth)

    val rfpClassificationModel = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    // DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val rfpPredict = testData.map { point =>
      val prediction = rfpClassificationModel.predict(point.features)
      (point.label, prediction)
    }
    //val predictionAndLabel = prediction zip(testData map(_ label))
    
    val predictionError = rfpPredict.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Validation Data : Prediction Error = " + predictionError)
    println("Classification Model - Decision Tree \n" + rfpClassificationModel.toDebugString)
    
    // need to get single final prediction using bagging or boosting option.
    
    // Load and parse the predict data file.
   /* val predictData = MLUtils.loadLibSVMFile(sc, args(1))
    
    // Predict Win/Loss for new RFPs
    val rfpPredictWL = predictData.map { point =>
      val prediction = rfpClassificationModel.predict(point.features)
      (prediction)
     }
     println("Prediction output for new RFPs:")
     rfpPredictWL.collect().foreach(println);*/

    // Save and load model
   // model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
  //  val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    
  }
}