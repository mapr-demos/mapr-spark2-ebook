package machinelearning

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.functions.{ concat, lit }

object Flight {

  case class Flight(_id: String, dofW: Integer, carrier: String, origin: String,
    dest: String, crsdephour: Integer, crsdeptime: Double, depdelay: Double,
    crsarrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Double)
    extends Serializable

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("dofW", IntegerType, true),
    StructField("carrier", StringType, true),
    StructField("origin", StringType, true),
    StructField("dest", StringType, true),
    StructField("crsdephour", IntegerType, true),
    StructField("crsdeptime", DoubleType, true),
    StructField("depdelay", DoubleType, true),
    StructField("crsarrtime", DoubleType, true),
    StructField("arrdelay", DoubleType, true),
    StructField("crselapsedtime", DoubleType, true),
    StructField("dist", DoubleType, true)
  ))

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("flightdelay").master("local[*]").getOrCreate()

    var file: String = "/mapr/demo.mapr.com/data/flights20170102.json"
    // var file = "maprfs:///data/flights20170102.json"
    var modeldirectory: String = "/mapr/demo.mapr.com/data/flightmodel"

    if (args.length == 1) {
      file = args(0)

    } else {
      System.out.println("Using hard coded parameters unless you specify the data file and test file. <datafile testfile>   ")
    }

    import spark.implicits._
    val df: Dataset[Flight] = spark.read.format("json").option("inferSchema", "false").schema(schema).load(file).as[Flight]

    println("training dataset")

    df.cache
    df.count()
    df.createOrReplaceTempView("flights")

    df.show
    val df1 = df.withColumn("orig_dest", concat($"origin", lit("_"), $"dest"))
    df1.show
    df1.createOrReplaceTempView("flights")

    // here we are looking at departure delays of 40 minutes, you  can change this to the value you want to look at
    df1.select($"orig_dest", $"depdelay")
      .filter($"depdelay" > 40)
      .groupBy("orig_dest")
      .count
      .orderBy(desc("count")).show(5)

    df.describe("dist", "depdelay", "arrdelay", "crselapsedtime").show

    // bucket by departure delay of 40 in order to count
    val delaybucketizer = new Bucketizer().setInputCol("depdelay")
      .setOutputCol("delayed").setSplits(Array(0.0, 40.0, Double.PositiveInfinity))

    val df2 = delaybucketizer.transform(df1)

    df2.createOrReplaceTempView("flights")

    df2.groupBy("delayed").count.show

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)

    // change this fraction to be proportional to the delays vs non delays that you are looking at
    val fractions = Map(0.0 -> .125, 1.0 -> 1.0) // 26
    val strain = trainingData.stat.sampleBy("delayed", fractions, 36L)
    strain.groupBy("delayed").count.show


    // column names for string types
    val categoricalColumns = Array("carrier", "origin", "dest", "dofW", "orig_dest")

    // used to encode string columns to number indices
    // Indices are fit to dataset
    val stringIndexers = categoricalColumns.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(strain)
    }

    // add a label column based on departure delay, here departure delay of 40
    val labeler = new Bucketizer().setInputCol("depdelay")
      .setOutputCol("label")
      .setSplits(Array(0.0, 40.0, Double.PositiveInfinity))

    // list of feature columns
    val featureCols = Array("carrierIndexed", "destIndexed",
      "originIndexed", "dofWIndexed", "orig_destIndexed",
      "crsdephour", "crsdeptime", "crsarrtime",
      "crselapsedtime", "dist")

    // combines a list of feature columns into a vector column
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val steps = stringIndexers ++ Array(labeler, assembler, rf)

    val pipeline = new Pipeline().setStages(steps)

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxBins, Array(100, 200))
      .addGrid(rf.maxDepth, Array(2, 4, 10))
      .addGrid(rf.numTrees, Array(5, 20))
      .addGrid(rf.impurity, Array("entropy", "gini"))
      .build()

    val evaluator = new BinaryClassificationEvaluator()

    // Set up 3-fold cross validation with paramGrid
    val crossvalidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid).setNumFolds(3)

    // fit the training data set and return a model
    val pipelineModel = crossvalidator.fit(strain)

    val featureImportances = pipelineModel
      .bestModel.asInstanceOf[PipelineModel]
      .stages(stringIndexers.size + 2)
      .asInstanceOf[RandomForestClassificationModel]
      .featureImportances

    assembler.getInputCols
      .zip(featureImportances.toArray)
      .sortBy(-_._2)
      .foreach {
        case (feat, imp) =>
          println(s"feature: $feat, importance: $imp")
      }

    val bestEstimatorParamMap = pipelineModel
      .getEstimatorParamMaps
      .zip(pipelineModel.avgMetrics)
      .maxBy(_._2)
      ._1
    println(s"Best params:\n$bestEstimatorParamMap")

    val predictions = pipelineModel.transform(testData)

    val areaUnderROC = evaluator.evaluate(predictions)

    println("areaUnderROC", areaUnderROC)

    val lp = predictions.select("label", "prediction")
    val counttotal = predictions.count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble

    val truep = lp.filter($"prediction" === 0.0)
      .filter($"label" === $"prediction").count() /
      counttotal.toDouble

    val truen = lp.filter($"prediction" === 1.0)
      .filter($"label" === $"prediction").count() /
      counttotal.toDouble

    val falsep = lp.filter($"prediction" === 0.0)
      .filter(not($"label" === $"prediction")).count() /
      counttotal.toDouble

    val falsen = lp.filter($"prediction" === 1.0)
      .filter(not($"label" === $"prediction")).count() /
      counttotal.toDouble

    val cor = truen + truep / falsep + falsen
    
    pipelineModel.write.overwrite().save(modeldirectory)

    println("ratio correct", ratioCorrect)

    println("true positive", truep)

    println("false positive", falsep)

    println("true negative", truen)

    println("false negative", falsen)

    val precision = truep / (truep + falsep)
    val recall = truep / (truep + falsen)
    val fmeasure = 2 * precision * recall / (precision + recall)
    val accuracy = (truep + truen) / (truep + truen + falsep + falsen)

    println("precision ", precision)
    println("recall " + recall)
    println("f_measure " + fmeasure)
    println("accuracy " + accuracy)
  }
}

