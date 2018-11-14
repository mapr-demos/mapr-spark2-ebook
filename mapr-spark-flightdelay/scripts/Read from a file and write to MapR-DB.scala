Read from a file and write to MapR-DB
create the mapr-db table :

maprcli table create -path /user/mapr/flighttable -tabletype json -defaultreadperm p -defaultwriteperm p

start the shell with : 
/opt/mapr/spark/spark-2.3.1/bin/spark-shell

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


 case class Flight(id: String,fldate: String,month:Integer, dofW: Integer, carrier: String, src: String,dst: String, crsdephour: Integer, crsdeptime: Integer, depdelay: Double, crsarrtime: Integer, arrdelay: Double, crselapsedtime: Double, dist: Double)
  
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


val spark: SparkSession = SparkSession.builder().appName("flightread").getOrCreate()
 
 var file ="maprfs:///user/mapr/data/flightdata2018.json"

 import spark.implicits._

 val df: Dataset[Flight] = spark.read.format("json").option("inferSchema", "false").schema(schema).load(file).as[Flight]

 df.take(10)

 df.saveToMapRDB(tableName, createTable = false, idFieldPath = "id")


