
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.functions.{concat, lit}


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

case class Flight(_id: String, dofW: Integer, carrier: String, origin: String, dest: String, crsdephour: Integer, crsdeptime: Double, depdelay: Double,crsarrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Double) extends Serializable 

var file = "/mapr/demo.mapr.com/data/flights20170102.json"
//var file = "maprfs:///data/flights20170102.json"

import spark.implicits._
val df: Dataset[Flight] = spark.read.format("json").option("inferSchema", "false").schema(schema).load(file).as[Flight]

df.count
df.cache
df.createOrReplaceTempView("flights")


df.show
val df1 = df.withColumn("orig_dest", concat($"origin", lit("_"), $"dest"))
df1.show
df1.createOrReplaceTempView("flights")

df1.select($"orig_dest", $"depdelay").filter($"depdelay" > 40).groupBy("orig_dest").count.orderBy(desc("count")).show(5)

val delaybucketizer = new Bucketizer().setInputCol("depdelay").setOutputCol("delayed").setSplits(Array(0.0, 15.0, Double.PositiveInfinity))
    
val df2 = delaybucketizer.transform(df1)

df2.createOrReplaceTempView("flights")

df2.groupBy("delayed").count.show

println(" average departure delay by day of the week")
spark.sql("SELECT dofW, avg(depdelay) as avgdelay FROM flights GROUP BY dofW ORDER BY avgdelay desc").show

    //Count of Departure Delays by Carrier (where delay=40 minutes)
println(" Count of Departure Delays by Carrier ")
df.filter($"depdelay" > 40).groupBy("carrier").count.orderBy(desc("count")).show(5)
spark.sql("select carrier, count(depdelay) from flights where depdelay > 40 group by carrier").show

println("what is the count of departure delay by origin airport where delay minutes >40")
spark.sql("select origin, count(depdelay) from flights where depdelay > 40 group by origin ORDER BY count(depdelay) desc").show

    // Count of Departure Delays by Day of the week
println("Count of Departure Delays by Day of the week, where delay minutes >40")
df.filter($"depdelay" > 40).groupBy("dofW").count.orderBy("dofW").show()
spark.sql("select dofW, count(depdelay)from flights where depdelay > 40 group by dofW order by dofW").show()
    
println("Count of Departure Delays by scheduled departure hour")
spark.sql("select crsdephour, count(depdelay) from flights where depdelay > 40 group by crsdephour order by crsdephour").show()

println("Count of Departure Delays by origin")
spark.sql("select origin, count(depdelay) from flights where depdelay > 40 group by origin ORDER BY count(depdelay) desc").show()



val fractions = Map(0.0 -> .26, 1.0 -> 1.0)  
val strain = df2.stat.sampleBy("delayed", fractions, 36L)
val Array(trainingData, testData) = strain.randomSplit(Array(0.7, 0.3), 5043)

strain.groupBy("delayed").count.show

trainingData.show

// column names for string types
val categoricalColumns = Array("carrier", "origin", "dest", "dofW", "orig_dest")

val stringIndexers = categoricalColumns.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(strain)
    }

val labeler = new Bucketizer().setInputCol("arrdelay").setOutputCol("label").setSplits(Array(0.0, 20.0, Double.PositiveInfinity))

val featureCols = Array("carrierIndexed", "destIndexed",
      "originIndexed", "dofWIndexed", "orig_destIndexed",
      "crsdephour", "crsdeptime", "crsarrtime",
      "crselapsedtime", "dist")

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")

val steps = stringIndexers ++ Array(labeler, assembler, rf)

val pipeline = new Pipeline().setStages(steps)

val paramGrid = new ParamGridBuilder().addGrid(rf.maxBins, Array(100, 200)).addGrid(rf.maxDepth, Array(2, 4, 10)).addGrid(rf.numTrees, Array(5, 20)).addGrid(rf.impurity, Array("entropy", "gini")).build()

val evaluator = new BinaryClassificationEvaluator()

val crossvalidator = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)

val pipelineModel = crossvalidator.fit(trainingData)

val featureImportances = pipelineModel.bestModel.asInstanceOf[PipelineModel].stages(stringIndexers.size + 2).asInstanceOf[RandomForestClassificationModel].featureImportances

assembler.getInputCols.zip(featureImportances.toArray).sortBy(-_._2).foreach{case (feat, imp) =>println(s"feature: $feat, importance: $imp")}

val bestEstimatorParamMap = pipelineModel.getEstimatorParamMaps.zip(pipelineModel.avgMetrics).maxBy(_._2)._1
println(s"Best params:\n$bestEstimatorParamMap")

val predictions = pipelineModel.transform(testData)

val areaUnderROC = evaluator.evaluate(predictions)

val lp = predictions.select("label", "prediction")
val counttotal = predictions.count()
val correct = lp.filter($"label" === $"prediction").count()
val wrong = lp.filter(not($"label" === $"prediction")).count()
val ratioWrong = wrong.toDouble / counttotal.toDouble
val ratioCorrect = correct.toDouble / counttotal.toDouble
val truep = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count() / counttotal.toDouble

val truen = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count() / counttotal.toDouble

val falsep = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble

val falsen = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble

val precision = truep / (truep + falsep)
val recall = truep / (truep + falsen)
val fmeasure = 2 * precision * recall / (precision + recall)
val accuracy = (truep + truen) / (truep + truen + falsep + falsen)

