package com.learn.kvs

import java.io.FileInputStream
import java.util.Properties

import scala.io.Source

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext

import com.databricks.spark.csv.CsvContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator



object RFPipelineClassification {

	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Random forest Classification").setMaster("local[*]")
			val sc = new SparkContext(conf)
			val sqlc = new SQLContext(sc)
			val csvSc = new CsvContext(sqlc)

			//Read parameters from property file
			val properties = new Properties()
			properties.load(new FileInputStream(args(0))) //args(0) //C:/Users/sxk0hg8/workspace/scala/sparkMlibPOCs/src/main/resources/RFCmodelParam.conf
			val srcFile = properties.getProperty("srcFile")
			val schemaFile = properties.getProperty("schemaFile")
			val modelOut = properties.getProperty("modelOut")
			val maxBins = properties.getProperty("maxBins").split(",").map(_.toInt)
			val maxDepths = properties.getProperty("maxDepths").toString.split(",").map(_.toInt)
			val impurity = properties.getProperty("impurity").split(",")
			val featureSubsetStrategy = properties.getProperty("featureSubsetStrategy")
			val numTrees = properties.getProperty("numTrees").split(",").map(_.toInt)
			val splitRatio = properties.getProperty("splitRatio").split(",").map(_.toDouble)
			val numFolds = properties.getProperty("numFolds").toInt

			Logger.getLogger("org").setLevel(Level.OFF)
			Logger.getLogger("akka").setLevel(Level.OFF)

			// Load and parse the training data file.
			val fileDF = sqlc.csvFile(srcFile,true)
			val columns = Source.fromFile(schemaFile).getLines().take(1).next().split(",")
			val filterRDD = fileDF.select(columns.head,columns.tail:_*)

			//Dataframe - Label, Vector
			import sqlc.implicits._ 
			val lableObj = filterRDD.map{ row => val arrayL = new ArrayBuffer[Double]()
			                              var lable:String = null
			                              for(column <- columns){
					                              if(lable == null){
						                                 lable =row.getAs[String](column)
					                              }else{
							                               arrayL +=(row.getAs[String](column).toDouble)
					                              }
			                              }                              
					                          val array = arrayL.toArray 
					                          LabeledPoint(lable.toInt, Vectors.dense(array.head,array.tail:_*))
		                            }.toDF()

			//Transformer - Identifies the number class
			val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")

			val Array(trainingData, testData) = lableObj.randomSplit(splitRatio)

			//RFC Algorithm - Estimator
			val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setFeatureSubsetStrategy(featureSubsetStrategy) //.setImpurity(impurity)
			
			//Pipeline
			val pipeline = new Pipeline().setStages(Array(labelIndexer,rf))

			//ParamGridBuilder
			val paramGrid = new  ParamGridBuilder().addGrid(rf.maxBins, maxBins).addGrid(rf.maxDepth, maxDepths)
					                  .addGrid(rf.impurity, impurity).addGrid(rf.numTrees, numTrees).build()

			val evaluater = new BinaryClassificationEvaluator().setLabelCol("indexedLabel")
			//CrossValidator
			val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluater).setEstimatorParamMaps(paramGrid).setNumFolds(numFolds)

			//Cross Validator Estimator - Passing entire dataset
			val cvModel = cv.fit(lableObj)

			//Retrieve bestModel 
			val bestPipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]

			//Pipeline - Transformer
			val predictions = bestPipelineModel.transform(testData)
			
			// Prediction Error - Option 2
			val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
			val accuracy = evaluator.evaluate(predictions)
			println("Test Error = " + (1.0 - accuracy))
			

			//Saving model into local directory
			//Check if accuracy is less 0.1
			// println("Classification Model - Decision Tree \n" + bestPipelineModel.)
			sc.parallelize(Seq(bestPipelineModel), 1).saveAsObjectFile(modelOut)
			if ( accuracy < 0.1 ) {
					println("Best Model has been identified and saving it for future use")
					//sc.parallelize(Seq(bestPipelineModel), 1).saveAsObjectFile(modelOut)
			} else {
					println("Try pass different parameters to algorithm to identify the best model")
			}

	 }
}