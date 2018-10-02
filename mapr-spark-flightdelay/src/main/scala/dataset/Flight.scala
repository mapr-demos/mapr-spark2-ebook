package dataset

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._

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

    val spark: SparkSession = SparkSession.builder().appName("flightdataset").master("local[*]").getOrCreate()

    var file: String = "/mapr/demo.mapr.com/data/flights20170102.json"
   // var file = "maprfs:///data/flights20170102.json"

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
    spark.catalog.cacheTable("flights")
    df.show

    println(" filter flights that departed at 10 AM. take 3")
    df.filter(flight => flight.crsdephour == 10).take(3)

    // group by and count by carrier 
    println(" group by and count by carrier ")
    df.groupBy("carrier").count().show()

    //count the departure delays greater than 40 minutes by destination, and sort them with the highest first. 
    println("count the departure delays greater than 40 minutes by destination, and sort them with the highest first")
    df.filter($"depdelay" > 40).groupBy("dest").count().orderBy(desc("count")).show(3)

    // top 5 dep delay 
    println(" longest departure delays ")
    df.select($"carrier", $"origin", $"dest", $"depdelay", $"crsdephour").filter($"depdelay" > 40).orderBy(desc("depdelay")).show(5)

    spark.sql("select carrier,origin, dest, depdelay,crsdephour, dist, dofW from flights where depdelay > 40 order by depdelay desc limit 5").show

    println(" average departure delay by Carrier")
    df.groupBy("carrier").agg(avg("depdelay")).show

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

    
    val delaybucketizer = new Bucketizer().setInputCol("depdelay").setOutputCol("delayed").setSplits(Array(0.0, 40.0, Double.PositiveInfinity))

    val df4 = delaybucketizer.transform(df)
    df4.groupBy("delayed").count.show
    df4.createOrReplaceTempView("flights")

    println("what is the count of departure delay and not delayed by origin")
    spark.sql("select origin, delayed, count(delayed) from flights group by origin, delayed order by origin").show

    println("what is the count of departure delay by dest")
    spark.sql("select dest, delayed, count(delayed) from flights where delayed=1 group by dest, delayed order by dest").show
    println("what is the count of departure delay by origin, dest")
    spark.sql("select origin,dest, delayed, count(delayed) from flights where delayed=1 group by origin,dest, delayed order by origin,dest").show
    println("what is the count of departure delay by dofW")
    spark.sql("select dofW, delayed, count(delayed) from flights where delayed=1 group by dofW, delayed order by dofW").show

    println("what is the count of departure delay by hour where delay minutes >40")
    spark.sql("select crsdephour, delayed, count(delayed) from flights where delayed=1 group by crsdephour, delayed order by crsdephour").show

    println("what is the count of departure delay carrier where delay minutes >40")
    spark.sql("select carrier, delayed, count(delayed) from flights where delayed=1 group by carrier, delayed order by carrier").show

  }
}

