package graphmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._
import org.apache.log4j.{ Level, Logger }
//import com.fasterxml.jackson.annotation.{ JsonIgnoreProperties, JsonProperty }

object WriteFlight {

  case class Flight(id: String, fldate: String, month: Integer, dofW: Integer, carrier: String, src: String, dst: String, crsdephour: Integer, crsdeptime: Integer, depdelay: Double, crsarrtime: Integer, arrdelay: Double, crselapsedtime: Double, dist: Double)

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

    var file = "maprfs:///mapr/demo.mapr.com/data/flightdata2018.json"
    var tableName: String = "/apps/flighttable"
    if (args.length == 2) {
      file = args(0)
      tableName = args(1)
    } else {
      System.out.println("Using hard coded parameters unless you specify the file and table names ")
    }

    val spark: SparkSession = SparkSession.builder().appName("querypayment").master("local[*]").getOrCreate()

    import spark.implicits._

    val df: Dataset[Flight] = spark.read.format("json").option("inferSchema", "false").schema(schema).load(file).as[Flight]

    df.take(10)

    df.saveToMapRDB(tableName, createTable = false, idFieldPath = "id")
    // to read 
 //   val df2 = spark.sparkSession.loadFromMapRDB[Flight](tableName, schema)

  }
}

