/* SimpleApp.scala */

object SimpleApp {
  def main(args: Array[String]) {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.explode   
    val spark = SparkSession.builder.appName("TripAnalysis").getOrCreate()
    import spark.implicits._
    val results_path = "s3a://results/"
    val paths = "s3a://trips/*"
    val tripDF = spark.read.option("multiline", "true").json(paths)
    //tripDF.printSchema()

    /*
    root
     |-- data: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- absVelocity: double (nullable = true)
     |    |    |-- angle: double (nullable = true)
     |    |    |-- cornerType: string (nullable = true)
     |    |    |-- datetime: string (nullable = true)
     |    |    |-- dbResponse: struct (nullable = true)
     |    |    |    |-- address: string (nullable = true)
     |    |    |    |-- country: string (nullable = true)
     |    |    |    |-- county: string (nullable = true)
     |    |    |    |-- dirTravel: string (nullable = true)
     |    |    |    |-- distFromPoint: double (nullable = true)
     |    |    |    |-- functionalClass: string (nullable = true)
     |    |    |    |-- heuristicSpeedLimit: boolean (nullable = true)
     |    |    |    |-- linkID: string (nullable = true)
     |    |    |    |-- matchDirection: string (nullable = true)
     |    |    |    |-- matchLatitude: double (nullable = true)
     |    |    |    |-- matchLongitude: double (nullable = true)
     |    |    |    |-- roadType: string (nullable = true)
     |    |    |    |-- speedLimit: double (nullable = true)
     |    |    |    |-- urban: boolean (nullable = true)
     |    |    |-- dist: double (nullable = true)
     |    |    |-- lat: double (nullable = true)
     |    |    |-- long: double (nullable = true)
     |    |    |-- radAccel: double (nullable = true)
     |    |    |-- radii: double (nullable = true)
     |    |    |-- spdLim: double (nullable = true)
     |    |    |-- tangAccel: double (nullable = true)
     |    |    |-- timeSecs: double (nullable = true)
     */

    //tripDF.show()
    //linksDF.show()
    //linksDF.printSchema()
    //linksDF2.show()
    //linksDF2.printSchema()
    //tDF.show()
    //tDF.printSchema()
    val linksDF = tripDF.select(explode($"data").as("data"))
    val linksDF2 = linksDF.select("data.dbResponse.linkID", "data.absVelocity")
    // create a temporary view using the DataFrame
    linksDF2.createOrReplaceTempView("times")
    val tDF = spark.sql("SELECT CAST(linkID as LONG), absVelocity from times WHERE linkID IS NOT NULL AND absVelocity IS NOT NULL")
    /*
    * root
      |-- linkID: string (nullable = true)
      |-- absVelocity: double (nullable = true)
    */

    // TODO changze this into a single aggregate function
    val groupedDS = tDF.groupBy("linkID")
    val avgsDS = groupedDS.agg(
      "linkID" -> "count",
      "absVelocity" -> "avg"
    ).sort($"linkID".asc)
    //val avgsDS = tDF.groupBy("linkID").count().join(tDF.groupBy("linkID").avg(), "linkID")
    //avgsDS.show()

    avgsDS.coalesce(1).write.
    option("header", "true").
    csv(results_path + "results_49998")
    spark.stop()
  }
}
