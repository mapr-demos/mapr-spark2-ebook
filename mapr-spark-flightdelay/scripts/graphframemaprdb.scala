start the shell with : 
/opt/mapr/spark/spark-2.3.1/bin/spark-shell --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11

import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.graphframes._
import org.graphframes.lib.AggregateMessages

  case class Flight(id: String,fldate: String,month:Integer, dofW: Integer, 
  carrier: String, src: String,dst: String, crsdephour: Integer, crsdeptime: Integer, 
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

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._

 val spark: SparkSession = SparkSession.builder().appName("flightread").getOrCreate()
 var tableName = "/user/mapr/flighttable"
 
 // load payment dataset from MapR-DB 
val df = spark.sparkSession.loadFromMapRDB[Flight](tableName, schema)

df.show

var file2: String = "/user/mapr/data/airports.json"
val airports = spark.read.json(file2)
airports.createOrReplaceTempView("airports")
airports.count
airports.show


val graph = GraphFrame(airports, df)

graph.vertices.show
graph.edges.show

graph.vertices.count
graph.edges.count

val paths = graph.bfs.fromExpr("id = 'LAX'").toExpr("id = 'LGA'").maxPathLength(3).edgeFilter("carrier != 'UA'").run()
paths.show()
paths.filter("e0.crsarrtime<e1.crsdeptime-60 and e0.fldate=e1.fldate").select("e0.id","e1.id").show(5,false)

graph.triplets.show(3)
graph.find("(src)-[edge]->(dst)").show
// Which flight routes have the  longest distance ?
graph.edges.groupBy("src", "dst")
.max("dist").sort(desc("max(dist)")).show(4)

 //count of departure delays by Origin and destination.
graph.edges.filter(" delay > 40").groupBy("src", "dst").agg(count("depdelay").as("flightcount")).sort(desc("flightcount")).show(5)


// What are the longest delays for flights that are greater than 1500 miles in  distance?
graph.edges.filter("dist > 1500")
.orderBy(desc("depdelay")).show(3)

//What is the average delay for delayed flights departing from Boston?
graph.edges.filter("src = 'BOS' and delay > 1").groupBy("src", "dst").avg("depdelay").sort(desc("avg(delay)")).show

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


import org.graphframes.lib.AggregateMessages

val AM = AggregateMessages
val msgToSrc = AM.edge("depdelay")
val agg = { graph.aggregateMessages
  .sendToSrc(msgToSrc)    
  .agg(avg(AM.msg).as("avgdelay"))
  .orderBy(desc("avgdelay"))
  .limit(5) } 
agg.show()

graph.edges.groupBy("src", "dst","crsdephour").avg("depdelay").sort(desc("avg(delay)")).show(5)

graph.edges.groupBy("crsdephour").avg("depdelay").sort(desc("avg(delay)")).show(5)

graph.edges.filter("src = 'SFO' and delay > 1").groupBy("crsdephour").avg("depdelay").sort(desc("avg(delay)")).show(5)

    val flightroutecount = graph.edges.groupBy("src", "dst").count().orderBy(desc("count"))
    flightroutecount.show(10)

// motif search Motif Find Airports without direct flights
    val temp = GraphFrame(graph.vertices, flightroutecount)
    val res = temp.find("(a)-[]->(b); (b)-[]->(c); !(c)-[]->(a)").filter("c.id !=a.id")
    //res.select($"a", $"c").distinct.show
    res.show

// bfs lax lga direct
val paths = graph.bfs.fromExpr("id = 'LAX'").toExpr("id = 'LGA'").maxPathLength(1).run()
paths.show()
// motif search connecting flights lax-lga
 graph.find("(a)-[ab]->(b); (b)-[bc]->(c)").filter("a.id = 'LAX'").filter("c.id = 'LGA'").limit(4).show    
// bfs lax to lga pathlength2
 graph.bfs.fromExpr("id = 'LAX'").toExpr("id = 'LGA'").maxPathLength(2).run().limit(4).show

//Computes shortest paths from each Airport to LGA
 val results = graph.shortestPaths.landmarks(Seq("LGA")).run()

val paths = graph.find("(a)-[ab]->(b); (b)-[bc]->(c)").filter("(a.id =='LAX') and (b.id =='IAH') and (c.id =='LGA') and (bc.arrdelay>40) and (ab.arrdelay>40)and(bc.crsdephour + bc.depdelay > ab.crsdephour + ab.arrdelay ) and (ab.fldate == bc.fldate)").limit(20)


results.select("id", "distances").show()

