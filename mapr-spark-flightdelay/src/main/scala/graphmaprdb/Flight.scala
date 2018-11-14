package graphmaprdb

import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.graphframes._
import org.graphframes.lib.AggregateMessages

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._

object Flight {

  case class Flight(id: String, fldate: String, month: Integer, dofW: Integer,
    carrier: String, src: String, dst: String, crsdephour: Integer, crsdeptime: Integer,
    depdelay: Double, crsarrtime: Integer, arrdelay: Double, crselapsedtime: Double, dist: Double)

  val schema = StructType(Array(
    StructField("id", StringType, true),
    StructField("fldate", StringType, true),
    StructField("month", IntegerType, true),
    StructField("dofW", IntegerType, true),
    StructField("carrier", StringType, true),
    StructField("src", StringType, true),
    StructField("dst", StringType, true),
    StructField("crsdephour", IntegerType, true),
    StructField("crsdeptime", IntegerType, true),
    StructField("depdelay", DoubleType, true),
    StructField("crsarrtime", IntegerType, true),
    StructField("arrdelay", DoubleType, true),
    StructField("crselapsedtime", DoubleType, true),
    StructField("dist", DoubleType, true)
  ))

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("flightgraphframes").master("local[*]").getOrCreate()

    var tableName: String = "/apps/flighttable"
    var file2: String = "/mapr/demo.mapr.com/data/airports.json"

    if (args.length == 2) {
      tableName = args(0)
      file2 = args(1)

    } else {
      System.out.println("Using hard coded parameters unless you specify the 2 input  files. <file file>   ")
    }

    import com.mapr.db._
    import com.mapr.db.spark._
    import com.mapr.db.spark.impl._
    import com.mapr.db.spark.sql._
    val df = spark.sparkSession.loadFromMapRDB[Flight](tableName, schema)

    df.createOrReplaceTempView("flights")

    val airports = spark.read.json(file2)
    airports.createOrReplaceTempView("airports")
    airports.show

    val graph = GraphFrame(airports, df)

    graph.vertices.show
    graph.edges.show

    // What are the longest delays for  flights  that are greater than  1500 miles in  distance?
    println("What are the longest delays for  flights  that are greater than  1500 miles in  distance?")
    graph.edges.filter("dist > 1500").orderBy(desc("depdelay")).show(5)

    // show the longest distance routes
    graph.edges.groupBy("src", "dst")
      .max("dist").sort(desc("max(dist)")).show(4)

    // Which flight routes have the highest average delay  ?
    graph.edges.groupBy("src", "dst").avg("depdelay").sort(desc("avg(delay)")).show(5)

    //count of departure delays by Origin and destination.  
    graph.edges.filter(" delay > 40").groupBy("src", "dst").agg(count("depdelay").as("flightcount")).sort(desc("flightcount")).show(5)

    // What are the longest delays for flights that are greater than 1500 miles in  distance?
    graph.edges.filter("dist > 1500")
      .orderBy(desc("depdelay")).show(3)

    //What is the average delay for delayed flights departing from Boston?
    graph.edges.filter("src = 'BOS' and delay > 1")
      .groupBy("src", "dst").avg("depdelay").sort(desc("avg(delay)")).show

    //which airport has the most incoming flights? The most outgoing ?
    graph.inDegrees.orderBy(desc("inDegree")).show(3)

    graph.outDegrees.orderBy(desc("outDegree")).show(3)

    //What are the highest degree vertexes(most incoming and outgoing flights)?
    graph.degrees.orderBy(desc("degree")).show()

    //What are the 4 most frequent flights in the dataset ? 
    graph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(4)

    // use pageRank
    val ranks = graph.pageRank.resetProbability(0.15).maxIter(10).run()

    // ranks.vertices.orderBy($"pagerank".desc).show()
    ranks.vertices.show()

    val AM = AggregateMessages
    val msgToSrc = AM.edge("depdelay")
    val agg = {
      graph.aggregateMessages
        .sendToSrc(msgToSrc)
        .agg(avg(AM.msg).as("avgdelay"))
        .orderBy(desc("avgdelay"))
        .limit(5)
    }
    agg.show()

    val flightroutecount = graph.edges.groupBy("src", "dst").count().orderBy(desc("count"))
    flightroutecount.show(10)

    println("Motif Find Airports without direct flights")
    val temp = GraphFrame(graph.vertices, flightroutecount)
    val res = temp.find("(a)-[]->(b); (b)-[]->(c); !(c)-[]->(a)").filter("c.id !=a.id")
    //res.select($"a", $"c").distinct.show
    res.show
    println("bfs search lax lga maxpath=1")
    val paths = graph.bfs.fromExpr("id = 'LAX'").toExpr("id = 'LGA'").maxPathLength(1).run()
    paths.show()

    println("motif search connecting flights through lax lga ")
    graph.find("(a)-[ab]->(b); (b)-[bc]->(c)").filter("a.id = 'LAX'").filter("c.id = 'LGA'").limit(4).show

    println("bfs search lax lga maxpath=2")
    graph.bfs.fromExpr("id = 'LAX'").toExpr("id = 'LGA'").maxPathLength(2).run().limit(4).show

    println("Computes shortest paths from each Airport to LGA")
    val results = graph.shortestPaths.landmarks(Seq("LGA")).run()
    results.select("id", "distances").show()

    val path2 = graph.find("(a)-[ab]->(b); (b)-[bc]->(c)").filter("(a.id =='LAX') and (b.id =='IAH') and (c.id =='LGA') and (bc.arrdelay>40) and (ab.arrdelay>40)and(bc.crsdephour + bc.depdelay > ab.crsdephour + ab.arrdelay ) and (ab.fldate == bc.fldate)").limit(20)

    path2.show
  }

}

