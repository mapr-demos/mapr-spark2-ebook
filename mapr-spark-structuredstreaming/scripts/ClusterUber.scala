

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._



  case class Uber(dt: java.sql.Timestamp, lat: Double, lon: Double, base: String) extends Serializable

  val schema = StructType(Array(
    StructField("dt", TimestampType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true)
  ))



    var file: String = "/mapr/demo.mapr.com/data/uber.csv"
    var savedirectory: String = "/mapr/demo.mapr.com/data/ubermodel"
    var file2: String = "/mapr/demo.mapr.com/data/uberjson"

    import spark.implicits._

    val df: Dataset[Uber] = spark.read.format("csv").option("inferSchema", "false").schema(schema).option("header", "false").load(file).as[Uber]
   

    df.show
    df.schema
    val temp = df.select(df("base")).distinct
    temp.show

    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)
    df2.cache
    df2.show
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)

    // increase the iterations if running on a cluster (this runs on a 1 node sandbox)
    val kmeans = new KMeans().setK(10).setFeaturesCol("features").setMaxIter(10).setPredictionCol("cid").setSeed(1L)

    val model = kmeans.fit(trainingData)
    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    val clusters = model.summary.predictions
    clusters.createOrReplaceTempView("uber")
    clusters.show
    clusters.createOrReplaceTempView("uber")

    println("Which clusters had the highest number of pickups?")
    clusters.groupBy("cid").count().orderBy(desc("count")).show
    println("select cid, count(cid) as count from uber group by cid")
    spark.sql("select cid, count(cid) as count from uber group by cid").show

    println("Which cluster/base combination had the highest number of pickups?")
    clusters.groupBy("cid", "base").count().orderBy(desc("count")).show

    println("which hours of the day and which cluster had the highest number of pickups?")
    clusters.select(hour($"dt").alias("hour"), $"cid")
      .groupBy("hour", "cid").agg(count("cid")
      .alias("count")).orderBy(desc("count")).show
    println("SELECT hour(uber.dt) as hr,count(cid) as ct FROM uber group By hour(uber.dt)")
    spark.sql("SELECT hour(uber.dt) as hr,count(cid) as ct FROM uber group By hour(uber.dt)").show
    

    // to save the model 
    println("save the model")
    model.write.overwrite().save(savedirectory)
    // model can be  re-loaded like this
    // val sameModel = KMeansModel.load(savedirectory)
    //  
    // to save the categories dataframe as json data
    val res = spark.sql("select dt, lat, lon, base, cid as cid FROM uber order by dt")
    res.show
    //  res.coalesce(1).write.format("json").save(file2)


