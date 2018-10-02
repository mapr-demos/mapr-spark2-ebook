package graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.graphframes._
import org.graphframes.lib.AggregateMessages

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

    val spark: SparkSession = SparkSession.builder().appName("flightgraphframes").master("local[*]").getOrCreate()

   var file1: String = "/mapr/demo.mapr.com/data/flights20170102.json"
    var file2: String = "/mapr/demo.mapr.com/data/airports.json"

    if (args.length == 2) {
      file1 = args(0)
      file2 = args(1)

    } else {
      System.out.println("Using hard coded parameters unless you specify the 2 input  files. <file file>   ")
    }
    import spark.implicits._
    val df: Dataset[Flight] = spark.read.format("json").option("inferSchema", "false").schema(schema).load(file1).as[Flight]
  
    df.createOrReplaceTempView("flights")

    val flights = df.withColumnRenamed("_id", "id")
      .withColumnRenamed("origin", "src").withColumnRenamed("dest", "dst").withColumnRenamed("depdelay", "delay")

    val airports = spark.read.json(file2)
    airports.createOrReplaceTempView("airports")
    airports.show

    val graph = GraphFrame(airports, flights)

    graph.vertices.show
    graph.edges.show

    // What are the longest delays for  flights  that are greater than  1500 miles in  distance?
    println("What are the longest delays for  flights  that are greater than  1500 miles in  distance?")
    graph.edges.filter("dist > 1500").orderBy(desc("delay")).show(5)

    // show the longest distance routes
    graph.edges.groupBy("src", "dst")
      .max("dist").sort(desc("max(dist)")).show(4)

    // Which flight routes have the highest average delay  ?
    graph.edges.groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")).show(5)

    //count of departure delays by Origin and destination.  
    graph.edges.filter(" delay > 40").groupBy("src", "dst").agg(count("delay").as("flightcount")).sort(desc("flightcount")).show(5)

    // What are the longest delays for flights that are greater than 1500 miles in  distance?
    graph.edges.filter("dist > 1500")
      .orderBy(desc("delay")).show(3)

    //What is the average delay for delayed flights departing from Boston?
    graph.edges.filter("src = 'BOS' and delay > 1")
      .groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")).show

    //which airport has the most incoming flights? The most outgoing ?
    graph.inDegrees.orderBy(desc("inDegree")).show(3)

    graph.outDegrees.orderBy(desc("outDegree")).show(3)

    //What are the highest degree vertexes(most incoming and outgoing flights)?
    graph.degrees.orderBy(desc("degree")).show()

    //What are the 4 most frequent flights in the dataset ? 
    graph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(4)

    // use pageRank
    val ranks = graph.pageRank.resetProbability(0.15).maxIter(10).run()

    ranks.vertices.orderBy($"pagerank".desc).show()

    val AM = AggregateMessages
    val msgToSrc = AM.edge("delay")
    val agg = {
      graph.aggregateMessages
        .sendToSrc(msgToSrc)
        .agg(avg(AM.msg).as("avgdelay"))
        .orderBy(desc("avgdelay"))
        .limit(5)
    }
    agg.show()
  }
}

