package stream

// http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams.html

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._


/**
 * Consumes messages from a topic in MapR Streams using the Kafka interface,
 * enriches the message with  the k-means model cluster id and publishs the result in json format
 * to another topic
 * Usage: SparkKafkaConsumerProducer  <model> <topicssubscribe> <topicspublish>
 *
 *   <model>  is the path to the saved model
 *   <topics> is a  topic to consume from
 *   <topicp> is a  topic to publish to
 * Example:
 *    $  spark-submit --class com.sparkkafka.uber.SparkKafkaConsumerProducer --master local[2] \
 * mapr-sparkml-streaming-uber-1.0.jar /user/user01/data/savemodel  /user/user01/stream:ubers /user/user01/stream:uberp
 *
 *    for more information
 *    http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
 */

object StructuredStreamingConsumerParquet extends Serializable {

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

  def main(args: Array[String]): Unit = {

    var topic: String = "/apps/stream:flights"
    var writedir: String = "/mapr/demo.mapr.com/data/flighttable"
    var modeldirectory: String = "/mapr/demo.mapr.com/data/flightmodel"

    if (args.length == 3) {
      topic = args(0)
      modeldirectory = args(1)
      writedir = args(2)

    } else {
      System.out.println("Using hard coded parameters unless you specify topic model directory and table. <topic model table>   ")
    }

    val spark: SparkSession = SparkSession.builder().appName("stream").master("local[*]").getOrCreate()

    val model = CrossValidatorModel.load(modeldirectory)

    import spark.implicits._

    val df1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "maprdemo:9092")
      .option("subscribe", topic)
      .option("group.id", "testgroup")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .option("maxOffsetsPerTrigger", 1000)
      .load()

    println(df1.schema)

    println("Enrich Transformm Stream")

    val df2 = df1.select($"value" cast "string" as "json").select(from_json($"json", schema) as "data").select("data.*")

    val df3 = df2.withColumn("orig_dest", concat($"origin", lit("_"), $"dest"))

    val predictions = model.transform(df3)

    val lp = predictions.select($"_id", $"dofW", $"carrier", $"origin", $"dest",
      $"crsdephour", $"crsdeptime", $"crsarrtime", $"crselapsedtime", $"depdelay", $"arrdelay", $"dist",
      $"label", $"prediction".alias("pred_rf")) //.orderBy("origin")

    println("write stream")

    val query = lp.writeStream
      .format("parquet")
      .option("startingOffsets", "earliest")
      .option("path", writedir)
      .option("checkpointLocation", "/tmp/flp")
      .partitionBy("origin")
      .start()
    query.awaitTermination()

  }

}