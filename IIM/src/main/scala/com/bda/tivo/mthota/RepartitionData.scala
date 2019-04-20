package com.bda.tivo.mthota

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.bda.tivo.utils.commonUtils
import org.apache.spark.sql.functions._

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark._
import org.apache.spark.rdd.RDD




object RepartitionData {


  def main(args: Array[String]) {


	/********* SPark Config ********/
    val conf = new SparkConf().setAppName("bda03-pipeline")	//new configuration
	  val sc = new SparkContext(conf)	// new SparkContext

	  val sqlContext = new org.apache.spark.sql.SQLContext(sc) // SQLContext
    import sqlContext.implicits._

    val raw_watch_data = sqlContext.read.parquet(commonUtils.raw_path_channel_data)
    val raw_device = sqlContext.read.parquet(commonUtils.raw_path_device_data)
    raw_watch_data.printSchema()
    raw_watch_data.show(5)
    raw_watch_data.persist()
    raw_device.printSchema()


    raw_watch_data.registerTempTable("FT_WATCH")
    raw_device.registerTempTable("DEVICE")

    val final_df = sqlContext.sql("SELECT W.*,TIME_ZONE,SK_SYSTEM_ID FROM FT_WATCH W JOIN DEVICE D ON D.SK_DEVICE_ID = W.SK_DEVICE_ID")
    final_df.show(5)
    final_df.persist()

    final_df.repartition(10)
      .write
      .partitionBy("SK_SYSTEM_ID","EVENT_DATE")
      .mode(saveMode = "Overwrite")
      .option("header", "true")
      .parquet(commonUtils.partitioned_path_channel_data)




   /****** Spark Read ******/
    // val program_data = sqlContext.read.parquet(program_location)
        //
       //
      //    channel_jan_data.registerTempTable("CHANNEL_JAN") // register as temptable
     //    program_data.registerTempTable("PROGRAM") // register as temptable
//
//    //print schema
//    println( "Schema of CHANNEL Data: " )
//    channel_jan_data.printSchema()
//
//    println( "Schema of PROGRAM Data: " )
//    program_data.printSchema()
//
//    //read data from temptable to dataframe
//
//	//val read_data = program_data
//	//.select($"PROGRAM_ID",$"RUNTIME",$"PARENT_PROGRAM_ID")
//	//.withColumn("PARENT_PROGRAM_ID", when($"PARENT_PROGRAM_ID" ==='NULL', 0).otherwise($"PARENT_PROGRAM_ID"))
//	//.filter($"PARENT_PROGRAM_ID" !== "NULL")
//
//    // read_data.registerTempTable("FINAL_DATA") // register as temptable
//
//    //show the table data
//    //println( "Show top records : " )
//    /****** Spark SQL*********/
//    Logger.logInfo("Selecting columns from Program...")
//    val PROGRAM_SELECT = sqlContext.sql(
//    	"""
//    	select
//	    P.PROGRAM_ID,
//	    P.PARENT_PROGRAM_ID,
//	    P.MASTER_TITLE,
//	    P.CATEGORY_ID,
//	    P.SUBCATEGORY_ID,
//	    P.RELEASE_YEAR,
//	    P.SERIES_MASTER_YN,
//	    P.RUNTIME,
//	    P.EVENT_DATE,
//	    P.EPISODE_NUMBER,
//	    P.EPISODE_TITLE,
//	    P.ORIGINAL_ADT
//	    from
//    		PROGRAM P
//    	where
//    	EVENT_DATE >= '2017-12-31' and
//	    EVENT_DATE <= '2019-01-01'
//	    -- and PARENT_PROGRAM_ID not in  ('NULL')
//
//    	""")
//    Logger.logInfo("Selecting columns from Program...Done")
//    PROGRAM_SELECT.registerTempTable("PROGRAM_SELECT_TABLE") // register as temptable
//
//	//println( "Show top records : CHANNEL_JAN" )
//    //sqlContext.sql("select * from CHANNEL_JAN").show()
//
//    //println( "Show top records : " )
//    Logger.logInfo("Selecting columns from Channel...")
//    val CHANNEL_JAN_SELECT = sqlContext.sql(
//    	"""
//		 select *,
//		 CASE WHEN SURFTIME <= 300 then 1 else 0 end as SURFTIME_VIEWER
//		 from
//	    	CHANNEL_JAN
//	    where
//	    	EVENT_DATE >= 20171231 and
//		    EVENT_DATE <= 20190101
//
//    	""")
//
//	//import org.apache.spark.sql.expressions.Window
//	//val surf_part = Window.partitionBy('PROGRAM_ID).orderBy('SURFTIME_VIEWER.desc)
//	//val CHANNEL_JAN_SELECT = CHANNEL_JAN_SELECT_FIRST.select('*, sum('SURFTIME_VIEWER) over surf_part as "NO_OF_SURF_VIEWERS")
//
//    Logger.logInfo("Selecting columns from Channel...Done")
//	CHANNEL_JAN_SELECT.registerTempTable("CHANNEL_JAN_SELECT_TABLE") // register as temptable
//
//	//print sql output
//	println( "ACTIVITY_WATCH_INFO: " )
//    sqlContext.sql("""
//    	SELECT * from CHANNEL_JAN_SELECT_TABLE
//    	""").show()
//
//	//print sql output
//	println( "PROGRAM_INFO: " )
//    sqlContext.sql("""
//    	SELECT 'PROGRAM_ALL' as TYPE,count(distinct PROGRAM_ID) as PROGRAM_COUNT from PROGRAM
//    	union
//    	SELECT 'PROGRAM_2018' as TYPE,count(distinct PROGRAM_ID) as PROGRAM_COUNT from PROGRAM_SELECT_TABLE
//    	""").show()
//
//	//print sql output
//	println( "CHANNEL_INFO : " )
//    sqlContext.sql("""
//    	SELECT 'WATCH_ALL' as TYPE,count(distinct PROGRAM_ID) as PROGRAM_COUNT from CHANNEL_JAN
//    	union
//    	SELECT 'WATCH_2018' as TYPE,count(distinct PROGRAM_ID) as PROGRAM_COUNT from CHANNEL_JAN_SELECT_TABLE
//    	""").show()
//
//    //joining the data to form some calculated columns
//	//println( "Show top records for JOIN: " )
//    Logger.logInfo("Selecting columns from JOIN...")
//    val JOIN_DATA_SELECT = sqlContext.sql(
//    	"""
//    	select
//		    P.PROGRAM_ID,
//		    P.PARENT_PROGRAM_ID,
//		    P.MASTER_TITLE,
//		    P.CATEGORY_ID,
//		    P.SUBCATEGORY_ID,
//		    P.RELEASE_YEAR,
//		    P.SERIES_MASTER_YN,
//		    P.RUNTIME,
//		    P.EVENT_DATE,
//		    P.EPISODE_NUMBER,
//		    P.EPISODE_TITLE,
//		    P.ORIGINAL_ADT,
//
//		    sum(CH.SURFTIME_VIEWER) as NO_OF_SURF_VIEWERS,
//	    	sum(case when CH.DURATION > 0.7*P.RUNTIME then 1 else 0 end) as NO_OF_FULL_SHOW_VIEWERS,
//	    	count(distinct CH.SK_DEVICE_ID) as NO_WATCHERS,
//	    	avg(CH.SURFTIME) as AVG_SURF_TIME,
//	    	min(CH.EVENT_DATE) as MIN_EVENT_DATE,
//			max(CH.EVENT_DATE) as MAX_EVENT_DATE,
//			avg(CH.DURATION)/3600 as AVG_DURATION_HRS
//
//	    from
//	    	PROGRAM_SELECT_TABLE P
//	    	join
//    		CHANNEL_JAN_SELECT_TABLE CH
//    		on
//    		P.PROGRAM_ID = CH.PROGRAM_ID and
//    		cast(P.EVENT_DATE as DATE) = CH.EVENT_DATE
//    	group by
//	    	P.PROGRAM_ID,
//		    P.PARENT_PROGRAM_ID,
//		    P.MASTER_TITLE,
//		    P.CATEGORY_ID,
//		    P.SUBCATEGORY_ID,
//		    P.RELEASE_YEAR,
//		    P.SERIES_MASTER_YN,
//		    P.RUNTIME,
//		    P.EVENT_DATE,
//		    P.EPISODE_NUMBER,
//		    P.EPISODE_TITLE,
//		    P.ORIGINAL_ADT
//	    """)
//    Logger.logInfo("Selecting columns from JOIN...Done")
//    JOIN_DATA_SELECT.registerTempTable("JOIN_DATA_SELECT_TABLE") // register as temptable
//
//
//    println( "Show top records - JOIN_INFO : " )
//    sqlContext.sql(
//    	"""
//    	select
//    		PROGRAM_ID,
//    		PARENT_PROGRAM_ID,
//    		MASTER_TITLE,
//    		CATEGORY_ID,
//    		SUBCATEGORY_ID
//    		RUNTIME,
//    		ORIGINAL_ADT,
//    		NO_OF_SURF_VIEWERS,
//    		NO_OF_FULL_SHOW_VIEWERS,
//    		NO_WATCHERS,
//    		AVG_SURF_TIME,
//    		MIN_EVENT_DATE,
//    		MAX_EVENT_DATE,
//    		AVG_DURATION_HRS
//    	from
//    		JOIN_DATA_SELECT_TABLE
//    	order by
//    		NO_WATCHERS DESC
//    	""").show()
//
//
//    sqlContext.sql(
//    	"""
//    	select
//    		*
//    	from
//    		JOIN_DATA_SELECT_TABLE
//    	""").coalesce(1).write.format("parquet").mode("append").save("hdfs://bigdatalab/user/bda03tivo/Output/JOIN_RESULT_ALL_3.parquet")
//
//
//
//    println( "JOIN_PROGRAM_CHANNEL_INFO : " )
//    sqlContext.sql("""
//    	select 'TOTAL_RECORD_COUNT' as TYPE,count(*) as COUNT from JOIN_DATA_SELECT_TABLE
//    	union
//    	select 'DISTINCT_PROGRAM_AFTER_JOIN' as TYPE,count(distinct PROGRAM_ID) as COUNT from JOIN_DATA_SELECT_TABLE
//    	""").show()
//
//
//    println( "DIFFERENT CHANNEL INFO : " )
//    sqlContext.sql("""
//    	select DISTINCT CH.PROGRAM_ID
//    	from CHANNEL_JAN_SELECT_TABLE CH
//    	left join PROGRAM_SELECT_TABLE P
//    	on CH.PROGRAM_ID = P.PROGRAM_ID
//    	where P.PROGRAM_ID is null
//    	""").show()
//
//    println( "DIFFERENT CHANNEL INFO - COUNT : " )
//    sqlContext.sql("""
//    	select count(DISTINCT CH.PROGRAM_ID)
//    	from CHANNEL_JAN_SELECT_TABLE CH
//    	left join PROGRAM_SELECT_TABLE P
//    	on CH.PROGRAM_ID = P.PROGRAM_ID
//    	where P.PROGRAM_ID is null
//    	""").show()
//
//    //read_data.write.format.csv("ads.csv")
//    //read_data.write.format("com.databricks.spark.csv").save("/user/bda03tivo/ads_feb_test/mydata.csv")
//    Logger.logInfo("Stopping SparkContext...")
//    sc.stop()
//    Logger.logInfo("Stopping SparkContext...Done")
  }
}