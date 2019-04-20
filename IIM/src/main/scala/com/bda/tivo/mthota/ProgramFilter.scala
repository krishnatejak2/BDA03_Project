package com.bda.tivo.mthota

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.bda.tivo.utils.commonUtils
import com.bda.tivo.utils.commonUtils.getEventEndDate
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf




object ProgramFilter {
  val getHouroftheDay = udf[Int, String](commonUtils.getHouroftheDay)
  val getDayoftheMonth = udf[Int, String](commonUtils.getDayoftheMonth)
  val getDayOfWeek = udf[Int, String](commonUtils.getDayOfWeek)
  val getDayOfWeekAsText = udf[String, String](commonUtils.getDayOfWeekAsText)
  val getMonthoftheYear = udf[Int, String](commonUtils.getMonthoftheYear)
  val isHoliday = udf[String, String](commonUtils.isHoliday)
  val isWeekEnd = udf[Int, String](commonUtils.isWeekEnd)
  val dayPart = udf[String, String](commonUtils.getDayPart)
  val getWeekoftheYear = udf[String, String](commonUtils.getWeekoftheYear)
  val getAirDate = udf[String, String](commonUtils.getAirDate)
  val getAirTime = udf[String, String](commonUtils.getAirTime)
  val getEventEndDate = udf[String, String,String](commonUtils.getEventEndDate)

  def main(args: Array[String]) {


	/********* SPark Config ********/
    val conf = new SparkConf().setAppName("bda03-pipeline").setMaster("local[*]")//new configuration

	  val sc = new SparkContext(conf)	// new SparkContext

	  val sqlContext = new org.apache.spark.sql.SQLContext(sc) // SQLContext
    import sqlContext.implicits._

    val raw_program_data = sqlContext.read.parquet(commonUtils.raw_path_program_data)
    raw_program_data.registerTempTable("PROGRAM")
    raw_program_data.printSchema()

    val raw_campaign_program_data = sqlContext.read.parquet(commonUtils.raw_path_campaign_program_data)
    raw_campaign_program_data.registerTempTable("CAMPAIGN_PROGRAM")
    raw_campaign_program_data.printSchema()
    println(raw_campaign_program_data.count())


    var final_df = sqlContext.sql("SELECT * FROM PROGRAM WHERE EVENT_DATE >= '2018-01-01' AND EVENT_DATE!='NULL'")
    final_df.show(5)
    println("========= "+final_df.count())

    final_df=final_df.withColumn("hour",getHouroftheDay($"EVENT_DATE"))
                      .withColumn("day",getDayoftheMonth($"EVENT_DATE"))
                      .withColumn("week_text",getDayOfWeekAsText($"EVENT_DATE"))
                      .withColumn("week_encoded",getDayOfWeek($"EVENT_DATE"))
                      .withColumn("month",getMonthoftheYear($"EVENT_DATE"))
                      .withColumn("vwp_days",isHoliday($"EVENT_DATE"))
                      .withColumn("weekend",isWeekEnd($"EVENT_DATE"))
                      .withColumn("daypart",dayPart($"EVENT_DATE"))
                      .withColumn("weekOfTheYear",getWeekoftheYear($"EVENT_DATE"))
                      .withColumn("EVENT_END_DATE",getEventEndDate($"EVENT_DATE",$"RUNTIME"))
                      .withColumn("airStartDate",getAirDate($"EVENT_DATE"))
                      .withColumn("airStartTime",getAirTime($"EVENT_DATE"))
                      .withColumn("airEndDate",getAirDate(getEventEndDate($"EVENT_DATE",$"RUNTIME")))
                      .withColumn("airEndTime",getAirTime(getEventEndDate($"EVENT_DATE",$"RUNTIME")))


    final_df.show()

    final_df.persist()
    final_df.registerTempTable("PROGRAM")
    final_df = sqlContext.sql("SELECT p.*,CASE WHEN ISNULL(c.PROGRAM_ID) THEN 0 ELSE 1 END AS is_tivo_ads_promoted FROM PROGRAM p LEFT JOIN CAMPAIGN_PROGRAM c ON c.PROGRAM_ID = p.PROGRAM_ID")
    println(final_df.count())
    final_df.show()
    final_df.repartition(1)
      .write
      .mode(saveMode = "Overwrite")
      .option("header", "true")
      .parquet(commonUtils.partitioned_path_program_data+"parquet/")
    val groupbyDF = sqlContext.sql("SELECT airStartDate,COUNT(*) as count FROM PROGRAM WHERE EVENT_DATE >= '2018-01-01' AND EVENT_DATE!='NULL' GROUP BY airStartDate ORDER BY airStartDate")

    groupbyDF.show(30)

  }
}