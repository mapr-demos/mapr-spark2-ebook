package parquet


import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


import org.apache.log4j.{ Level, Logger }


object QueryFlight {



  case class Flight(_id: String, dofW: Integer, carrier: String, origin: String,
    dest: String, crsdephour: Integer, crsdeptime: Double, depdelay: Double,
    crsarrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Double, label: Double, pred_rf: Double)
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
    StructField("dist", DoubleType, true),
    StructField("label", DoubleType, true),
    StructField("pred_rf", DoubleType, true)
  ))

  def main(args: Array[String]) {


    var dir: String = "/mapr/demo.mapr.com/data/flighttable"
    if (args.length == 1) {
      dir = args(0)
    } else {
      System.out.println("Using hard coded parameters unless you specify the tablename ")
    }
    val spark: SparkSession = SparkSession.builder().appName("querypayment").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._
    // load payment dataset from parquet
  
    val fdf: Dataset[Flight] = spark.read.format("parquet").option("inferSchema", "false").schema(schema).load(dir).as[Flight]
  
   

    println("Flights from MapR-DB")
    fdf.show

    fdf.createOrReplaceTempView("flight")
    println("what is the count of predicted delay/notdelay for this dstream dataset")
    fdf.groupBy("pred_rf").count().show()

    println("what is the count of predicted delay/notdelay by scheduled departure hour")
    spark.sql("select crsdephour, pred_rf, count(pred_rf) from flight group by crsdephour, pred_rf order by crsdephour").show
    println("what is the count of predicted delay/notdelay by origin")
    spark.sql("select origin, pred_rf, count(pred_rf) from flight group by origin, pred_rf order by origin").show
    println("what is the count of predicted and actual  delay/notdelay by origin")
    spark.sql("select origin, pred_rf, count(pred_rf),label, count(label) from flight group by origin, pred_rf, label order by origin, label, pred_rf").show
    println("what is the count of predicted delay/notdelay by dest")
    spark.sql("select dest, pred_rf, count(pred_rf) from flight group by dest, pred_rf order by dest").show
    println("what is the count of predicted delay/notdelay by origin,dest")
    spark.sql("select origin,dest, pred_rf, count(pred_rf) from flight group by origin,dest, pred_rf order by origin,dest").show
    println("what is the count of predicted delay/notdelay by day of the week")
    spark.sql("select dofW, pred_rf, count(pred_rf) from flight group by dofW, pred_rf order by dofW").show
    println("what is the count of predicted delay/notdelay by carrier")
    spark.sql("select carrier, pred_rf, count(pred_rf) from flight group by carrier, pred_rf order by carrier").show

    val lp = fdf.select("label", "pred_rf")
    val counttotal = fdf.count()
    val label0count = lp.filter($"label" === 0.0).count()
    val pred0count = lp.filter($"pred_rf" === 0.0).count()
    val label1count = lp.filter($"label" === 1.0).count()
    val pred1count = lp.filter($"pred_rf" === 1.0).count()

    val correct = lp.filter($"label" === $"pred_rf").count()
    val wrong = lp.filter(not($"label" === $"pred_rf")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    val truep = lp.filter($"pred_rf" === 0.0)
      .filter($"label" === $"pred_rf").count() / counttotal.toDouble
    val truen = lp.filter($"pred_rf" === 1.0)
      .filter($"label" === $"pred_rf").count() / counttotal.toDouble
    val falsep = lp.filter($"pred_rf" === 0.0)
      .filter(not($"label" === $"pred_rf")).count() / counttotal.toDouble
    val falsen = lp.filter($"pred_rf" === 1.0)
      .filter(not($"label" === $"pred_rf")).count() / counttotal.toDouble

    println("ratio correct ", ratioCorrect)
    println("ratio wrong ", ratioWrong)
    println("correct ", correct)
    println("true positive ", truep)
    println("true negative ", truen)
    println("false positive ", falsep)
    println("false negative ", falsen)
  }
}

