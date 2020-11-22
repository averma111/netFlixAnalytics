import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object netflix {

  val URL = "jdbc:mysql://localhost:3306/employees"
  val DRIVER = "com.mysql.cj.jdbc.Driver"
  val USER = "root"
  val PASS = "Neoman#02"
  val FORMAT = "jdbc"

  def displyData(df: DataFrame): Unit = {
    df.cache()
    df.show(false)

  }

/*  def getTotalHour(duration: Column): Unit = {

    val colToStr = duration.cast("String")

    print(colToStr)

  }*/

  def cleanseData(df: DataFrame): DataFrame = {

    val dropSeq = Seq("Start Time", "Attributes", "Supplemental Video Type",
      "Device Type", "Bookmark", "Latest Bookmark", "Duration")
    val rawDf: DataFrame = df.withColumn(
      "Timestamp", col("Duration")
    ).drop(dropSeq: _*)
      .select(col("Profile Name").alias("Profile"),col("Title"),
        col("Country"),col("Timestamp")
      )

    val rawDfLocation = rawDf.withColumn(
      "Location", substring(col("Country"), 1, 2)
    ).drop("Country")
   // val time = rawDfLocation.select(col("Timestamp"))
    //getTotalHour(time)

    rawDf
  }


    def calculateDistinctTitle(df: DataFrame): DataFrame = {

      val distinctTitle :DataFrame = df.select(col("Profile"),
        col("Title"),col("Country")
      ).groupBy(col("Profile")
      ).agg(
        col("Profile"),countDistinct(col("Title"))
      )
     // distinctTitle

      val totalCount = distinctTitle.select(col("Profile")
        ,col("count(DISTINCT Title)").alias("DistinctTitles")
      )
      totalCount
   }

  def countTotalTitle(df: DataFrame): Long = {
      df.count()
  }

  def readDataMysql(spark: SparkSession, tablename: String): DataFrame = {

    val mysqlDb = spark.read
      .format(FORMAT)
      .option("url", URL)
      .option("driver", DRIVER)
      .option("dbtable", tablename)
      .option("user", USER)
      .option("password", PASS)
      .load()
    mysqlDb

  }

  def writeDataMysql(df: DataFrame, tablenameTarget: String, mode: String): Unit = {

    df.write
      .format(FORMAT)
      .mode(mode)
      .option("url", URL)
      .option("driver", DRIVER)
      .option("dbtable", tablenameTarget)
      .option("user", USER)
      .option("password", PASS)
      .save()
  }


  def main(args: Array[String]): Unit = {

    //Creating the spark session as entry point
    val spark = SparkSession.builder()
      .appName("netflix")
      .master("local[*]")
      .getOrCreate()

    //Reading raw data from csv file
    val rawDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/ViewingActivity.csv")

    val cleanDf = cleanseData(rawDf)

      //displyData(cleanDf)
    displyData(calculateDistinctTitle(cleanDf))

    //Total Title
   val totalTitle=countTotalTitle(cleanDf)
    print("TotalTitle " + totalTitle)
  }

}
