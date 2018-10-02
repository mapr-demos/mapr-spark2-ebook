//  To launch the shell
//  /opt/mapr/spark/spark-2.2.1/bin/spark-shell --master local[2] --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.graphframes._
import spark.implicits._


case class Airport(id: String, city: String) extends Serializable

val airports=Array(Airport("SFO","San Francisco"),Airport("ORD","Chicago"),Airport("DFW","Dallas Fort Worth"))

val vertices = spark.createDataset(airports).toDF
vertices.show


case class Flight(id: String, src: String,dst: String, dist: Double, delay: Double)

val flights=Array(Flight("SFO_ORD_2017-01-01_AA","SFO","ORD",1800, 40),Flight("ORD_DFW_2017-01-01_UA","ORD","DFW",800, 0),Flight("DFW_SFO_2017-01-01_DL","DFW","SFO",1400, 10))

val edges = spark.createDataset(flights).toDF
edges.show


val graph = GraphFrame(vertices, edges)


graph.vertices.show


graph.vertices.collect.foreach(println)

// number of airports
graph.vertices.count()

// number of routes 
graph.edges.count()

graph.edges.show

graph.inDegrees.show


graph.triplets.show

graph.degrees.show


//  queries on the edges DataFrame. 
// minimum distance
graph.edges.groupBy().min("dist").show
// maximum delay
graph.edges.groupBy().max("delay").show


//How many routes distance greater than 800?
graph.edges.filter("dist > 800").show

//which routes have delay greater than 0?
graph.edges.filter("delay > 0").collect.foreach(println)

//Sort and print out the longest distance routes
graph.edges.groupBy("src", "dst").max("dist").sort(desc("max(dist)")).show

// get the average delay for flights from DFW with a delay 
graph.edges.filter("src = 'DFW' and delay > 0").groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")).show

val results = graph.triangleCount.run()
results.select("id", "count").show()

val paths = graph.shortestPaths.landmarks(Seq("SFO", "DFW")).run()
paths.collect.foreach(println)


val results = graph.pageRank.resetProbability(0.15).tol(0.01).run()
results.vertices.show

results.edges.show
