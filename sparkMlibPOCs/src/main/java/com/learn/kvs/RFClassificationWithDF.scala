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
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.databricks.spark.csv.CsvContext
import org.apache.spark.ml.attribute.AttributeGroup

// renaming input files
//use training data to split training/validation
//test numfolds significance
object RFClassificationWithDF {
  def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Random forest Classification").setMaster("local")
			val sc = new SparkContext(conf)
			val sqlc = new SQLContext(sc)
			val csvSc = new CsvContext(sqlc)

			//Read parameters from property file
			val params = new Properties()
			params.load(new FileInputStream("C:/Users/sxk0hg8/workspace/scala/sparkMlibPOCs/src/main/resources/RFCmodelParam.conf")) //args(0) //C:/Users/sxk0hg8/workspace/scala/sparkMlibPOCs/src/main/resources/RFCmodelParam.conf
			val rfpTraingData = params.getProperty("rfpTraingData")
			val schemaFile = params.getProperty("schemaFile")
			val rfpValidataionData = params.getProperty("rfpValidataionData")
			val modelOut = params.getProperty("modelOut")
			val maxBins = params.getProperty("maxBins").split(",").map(_.toInt)
			val maxDepths = params.getProperty("maxDepths").toString.split(",").map(_.toInt)
			val impurity = params.getProperty("impurity").split(",")
			val featureSubsetStrategy = params.getProperty("featureSubsetStrategy")
			val numTrees = params.getProperty("numTrees").split(",").map(_.toInt)
			val splitRatio = params.getProperty("splitRatio").split(",").map(_.toDouble)
			val numFolds = params.getProperty("numFolds").toInt
			
			Logger.getLogger("org").setLevel(Level.OFF)
			Logger.getLogger("akka").setLevel(Level.OFF)

			// Load influenced columns for prediction
			var columns = Source.fromFile(schemaFile).getLines().take(1).next().split(",")
			val label = columns(0) // extracting label column out
			val nonCatColumns = columns.drop(1)
			
			// Load data file to get schema.
			val sourceFile = sqlc.csvFile(filePath=rfpTraingData, useHeader=true, delimiter=',', inferSchema=true)
			
			//build the schema, modify non categorical influenced attributes to double
			val sourceSchema = sourceFile.schema
			val schemaSt = sourceSchema.map{ field =>
			                                       if(nonCatColumns.contains(field.name) ){ // ignore labeled column
			                                            StructField(field.name, DoubleType, nullable=true)
			                                       }else{
			                                                field
			                                       }
			                                } 
			val newSchema = StructType(schemaSt)
			
			//Read source file and validation data.
				val TotalDataDF = sqlc.read
                          .format("com.databricks.spark.csv")
                          .option("header", "true") // Use first line of all files as header
                          .schema(newSchema)
                          .load(rfpTraingData).cache
                          
		  val Array(trainingData, testData) = TotalDataDF.randomSplit(splitRatio)
		  
/*		As training and test data get it from same file with random split commenting this	
 *    val validateDF = sqlc.read
                          .format("com.databricks.spark.csv")
                          .option("header", "true") // Use first line of all files as header
                          .schema(newSchema)
                          .load(predFile).cache*/
		
			//Transformer - Identifies the number class for future use, when accept non numeric columns
		/*	 val stringIndexers = categoricalFeatColNames.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(allData)
    }*/

    // Indexing the labeled column
		val labelIndexer = new StringIndexer()
		                             .setInputCol(label) // Assuming First column is labeled column.
		                             .setOutputCol("indexedLabel")
		                             .fit(trainingData)
		  // define vector assembler with prediction attributes.
      val assembler = new VectorAssembler()
                              .setInputCols(Array(nonCatColumns: _*))
                              .setOutputCol("Features")
                            
      //RFC Algorithm  with specifications
			val randomForest = new RandomForestClassifier()
			                    .setLabelCol ("indexedLabel")
			                    .setFeaturesCol("Features")
			                    .setFeatureSubsetStrategy(featureSubsetStrategy)
			                    .setSeed(12345) // Random number to make sure prediction is same
			                    
			//Re-labeling back to values
			val labelConverter = new IndexToString()
                             .setInputCol("prediction")
                             .setOutputCol("predictedLabel")
                             .setLabels(labelIndexer.labels)
      
       // Extract features label information
       val dummyPipeline = new Pipeline().setStages(Array(labelIndexer, assembler))
       val out = dummyPipeline.fit(trainingData).transform(trainingData)
       val attrGroup = AttributeGroup.fromStructField(out.schema("Features"))

       val attributes = attrGroup.attributes.get
               println(s"Num features = ${attributes.length}")
               attributes.zipWithIndex.foreach { case (attr, idx) =>
                                                    println(s" - $idx = ${attr.name}")
                                               }
			// define the order of the operations to be performed
      val pipeline = new Pipeline().setStages(Array(labelIndexer,assembler,randomForest,labelConverter))
               
	    //ParamGridBuilder
			val paramGrid = new  ParamGridBuilder().addGrid(randomForest.maxBins, maxBins)
			                            .addGrid(randomForest.maxDepth, maxDepths)
					                        .addGrid(randomForest.impurity, impurity)
					                        .addGrid(randomForest.numTrees, numTrees)
					                       // .addGrid(randomForest.seed, 12345L)
					                        .build()
					                        
			 val evaluator = new BinaryClassificationEvaluator().setLabelCol ("indexedLabel")		                        
			
			 //CrossValidator
			val cv = new CrossValidator().setEstimator(pipeline)
			                    .setEvaluator(evaluator)
			                    .setEstimatorParamMaps(paramGrid)
			                    .setNumFolds(numFolds)

			// Passing entire training dataset
			val cvModel = cv.fit(trainingData)

			//Retrieve bestModel 
			val bestPipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
	
			// printing the model tree
			val rfmodel = bestPipelineModel.stages.collect { case t: RandomForestClassificationModel => t}.headOption
			rfmodel.foreach{tree =>
			                // println(tree.trees.mkString)               
			                 println(tree.toDebugString)
			               }
			//println(rfmodel.toDebugString)
			
			//Pipeline - Transformer
			val predictions = bestPipelineModel.transform(testData)
			//predictions.show(30)
			//predict with new result
			predictions.select("BIDS_ID",s"$label", "predictedLabel","probability").coalesce(1).show(30)
				
			// calculate Prediction Error
			val Tevaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
			val accuracy = Tevaluator.evaluate(predictions)
			println("Test Error = " + (1.0 - accuracy))
			
			//saving the prediction data
		  predictions.select("BIDS_ID",s"$label", "predictedLabel","probability")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save("c:/spark/data/rfcPredict")
  }
}