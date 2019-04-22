package com.bda.tivo.mthota

import com.bda.tivo.utils.commonUtils
import org.apache.spark.{SparkConf, SparkContext}
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
  val getAirYear = udf[Int, String](commonUtils.getYear)
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

    val raw_campaign_data = sqlContext.read.parquet(commonUtils.raw_path_campaign_data)
    raw_campaign_data.registerTempTable("CAMPAIGN")
    raw_campaign_data.printSchema()

    var campaign_df = sqlContext.sql("SELECT DISTINCT cp.PROGRAM_ID, EFFECTIVE_DATE,EVENT_DATE,RUNTIME   FROM CAMPAIGN_PROGRAM cp JOIN CAMPAIGN c ON c.PROGRAM_ID = cp.PROGRAM_ID WHERE TIME_ZONE = 'ET'")
    campaign_df.show()
    campaign_df = campaign_df.filter($"EVENT_DATE".isNotNull)
    campaign_df.show()
    campaign_df =  campaign_df.withColumn("EVENT_END_DATE",getEventEndDate($"EVENT_DATE",$"RUNTIME"))
    campaign_df.printSchema()
    campaign_df.show()
    campaign_df.registerTempTable("AD_CAMPAIGNS")

    campaign_df = sqlContext.sql("SELECT PROGRAM_ID,count(*) as total_ad_slots,SUM(CASE WHEN AD_SLOTS = 'RR' THEN 1 ELSE 0 END) as RR_slots, SUM(CASE WHEN AD_SLOTS = 'WN' THEN 1 ELSE 0 END) as WN_slots,SUM(CASE WHEN AD_SLOTS = 'VOD' THEN 1 ELSE 0 END)  as VOD_slots FROM (SELECT c.PROGRAM_ID, CASE WHEN c.EFFECTIVE_DATE < EVENT_DATE THEN 'RR' WHEN c.EFFECTIVE_DATE > EVENT_END_DATE THEN 'VOD' ELSE 'WN' END AS AD_SLOTS FROM AD_CAMPAIGNS c) t GROUP BY PROGRAM_ID")
    println(campaign_df.select("PROGRAM_ID").distinct().count())
    campaign_df.show()
    campaign_df.registerTempTable("AD_CAMPAIGNS")






    var final_df = sqlContext.sql("SELECT * FROM PROGRAM WHERE EVENT_DATE >= '2018-01-01' AND EVENT_DATE!='NULL'")
    final_df.show(5)
    println("========= "+final_df.count())
    var err_df = sqlContext.sql("SELECT * FROM PROGRAM WHERE  EVENT_DATE='NULL' AND (MASTER_TITLE != 'NULL' AND TRIM(MASTER_TITLE) != '') AND (EPISODE_TITLE  IS NOT NULL AND EPISODE_TITLE != 'NULL' AND TRIM(EPISODE_TITLE) != '') AND RELEASE_YEAR = 2018 AND CATEGORY_ID IN (3,5,8)")
    err_df.show(25)
    err_df.repartition(1).write.mode(saveMode = "Overwrite")
      .option("header", "true").parquet(commonUtils.partitioned_path_program_data+"toimpute/")
    println(err_df.count())

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
                      .withColumn("AirYear",getAirYear($"EVENT_DATE"))


    final_df.show()

    final_df.persist()
    final_df.registerTempTable("PROGRAM")
    final_df = sqlContext.sql("SELECT p.*,CASE WHEN ISNULL(c.PROGRAM_ID) THEN 0 ELSE 1 END AS is_tivo_ads_promoted,CASE WHEN ISNULL(total_ad_slots) THEN 0 ELSE total_ad_slots END  as total_ad_slots,CASE WHEN ISNULL(RR_slots) THEN 0 ELSE RR_slots END  as RR_slots,CASE WHEN ISNULL(VOD_slots) THEN 0 ELSE VOD_slots END  as VOD_slots,CASE WHEN ISNULL(WN_slots) THEN 0 ELSE WN_slots END  as WN_slots  FROM PROGRAM p LEFT JOIN AD_CAMPAIGNS c ON c.PROGRAM_ID = p.PROGRAM_ID")
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