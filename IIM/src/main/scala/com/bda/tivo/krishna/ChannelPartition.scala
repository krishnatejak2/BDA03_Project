package com.bda.tivo.krishna

import com.bda.tivo.utils.commonUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.SparkConf

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Logger{
  def methodName() = Thread.currentThread().getStackTrace()(3).getMethodName
  val log = LogManager.getRootLogger
  val header = "Your project Description"
  def logInfo(msg: String= "")= if(msg.length > 0) log.info(String.format("(%s) - %s", methodName(), msg)) else log.info("")
  def logDebug(msg: String= "")= if(msg.length > 0) log.debug(String.format("(%s) - %s", methodName(), msg)) else log.debug("")
  def logWarn(msg: String= "")= if(msg.length > 0) log.warn(String.format("(%s) - %s", methodName(), msg)) else log.warn("")
  def logError(msg: String= "")= if(msg.length > 0) log.error(String.format("(%s) - %s", methodName(), msg)) else log.error("")
  def title = log.warn(header)
}


object ChannelPartition {

  def main(args: Array[String]) {

  	Logger.logInfo("Successfully created Spark Configuration Object")
	
    /********* SPark Config ********/
    // val conf = new SparkConf().setAppName("channel-data-app")	//new configuration
    val conf = new SparkConf().setAppName("bda03tivo-channelApp") //new configuration
	val sc = new SparkContext(conf)	// new SparkContext
	
    Logger.logInfo("Successfully created Spark Context")
	
    val sqlContext = new org.apache.spark.sql.SQLContext(sc) // SQLContext
    import sqlContext.implicits._ // changes RDD to DF
    //import org.apache.spark.sql.expressions.Window
    
    Logger.logInfo("Reading the PROGRAM file...")
        val raw_program_data = sqlContext.read.parquet(commonUtils.raw_path_program_data_with_prev_prog)
        raw_program_data.registerTempTable("PROGRAM")
        raw_program_data.printSchema()                
    Logger.logInfo("Reading the Program file...Done")
    
    Logger.logInfo("Reading the DEVICE file...")
        val raw_device_data = sqlContext.read.parquet(commonUtils.raw_path_device_data)
        raw_device_data.registerTempTable("DEVICE")
        raw_device_data.printSchema()      
    Logger.logInfo("Reading the DEVICE file...Done")

    Logger.logInfo("Reading the Channel file for All Months...")
        val raw_channel_data = sqlContext.read.parquet(commonUtils.raw_path_channel_data)
        raw_channel_data.registerTempTable("WATCH")
        raw_channel_data.printSchema()        
    Logger.logInfo("Reading the Channel file for All Months...Done")    

    Logger.logInfo("Joining WATCH and DEVICE info...")
    val device_watch_df = sqlContext.sql(
        """
        select 
            W.*, 
            D.TIME_ZONE, 
            D.SK_SYSTEM_ID
        from 
            WATCH W
            left join 
            DEVICE D 
            on W.SK_DEVICE_ID = D.SK_DEVICE_ID
        """)
    Logger.logInfo("Joining WATCH and DEVICE info...Done")

    println( "Schema of DEVICE_WATCH : After joining DEVICE & WATCH info" )
    device_watch_df.printSchema()   
    device_watch_df.show()
    // device_watch_df.persist()
    device_watch_df.registerTempTable("DEVICE_WATCH_TABLE")
    println("Device_Watch Count:" )
    println(device_watch_df.count())

    //sqlContext.udf.register("add_seconds", (datetime : java.sql.Timestamp, seconds : Int) => {new java.sql.Timestamp(datetime.getTime() + seconds*1000 )});

    Logger.logInfo("Selecting columns from Program...")
    val program_select_df = sqlContext.sql(
        """
            select 
                P.PROGRAM_ID,
                P.PARENT_PROGRAM_ID,
                P.MASTER_TITLE,
                P.CATEGORY_ID,
                P.SUBCATEGORY_ID,
                P.RELEASE_YEAR,
                P.SERIES_MASTER_YN,
                P.RUNTIME,
                P.EVENT_DATE,
                P.EPISODE_NUMBER,
                P.EPISODE_TITLE,
                P.ORIGINAL_ADT,
                P.PROGRAM_EPISODE_RANK,
                P.PREV_PROGRAM_ID,
                cast(
                    case when P.ORIGINAL_ADT in ('NULL') then 
                        (case when P.EVENT_DATE in ('NULL') then null else P.EVENT_DATE end)
                    else P.ORIGINAL_ADT end as TIMESTAMP
                ) as PROGRAM_STARTTIME
            from 
                PROGRAM P 
            where
                EVENT_DATE <> 'NULL' and EVENT_DATE >= '2018-01-01'
                
        """)
    Logger.logInfo("Selecting columns from Program...Done")
   
    println( "Schema of PROGRAM Data: After some changes" )
    program_select_df.printSchema()      
    program_select_df.show()
    // program_select_df.persist()
    program_select_df.registerTempTable("PROGRAM_TABLE")
    println( "Program Count:" )
    println(program_select_df.count())


    Logger.logInfo("Selecting columns from Channel...")
    val watch_select_df = sqlContext.sql(
        """
                select 
                     *, 
                     DATE_FORMAT( CAST( UNIX_TIMESTAMP(cast(EVENT_DATE as string),'yyyyMMdd') as TIMESTAMP ), 'yyyy-MM-dd'  ) as EVENT_DATE_DATEFORMAT,
                     concat_ws(':', cast(floor(EVENT_TIME/3600) as string), cast (floor((EVENT_TIME % 3600)/60) as string), cast(floor((EVENT_TIME % 60))as string) ) as TIME_CONCAT,
                     CASE WHEN SURFTIME <= 300 then 1 else 0 end as SURFTIME_VIEWER
                 from 
                    DEVICE_WATCH_TABLE
                where
                    EVENT_DATE >= 20180101 and 
                    EVENT_DATE <= 20181231
        """)
    Logger.logInfo("Selecting columns from Channel...Done")

    println( "Schema of WATCH Data: After some changes" )
    watch_select_df.printSchema()      
    watch_select_df.show()
    // watch_select_df.persist()
    watch_select_df.registerTempTable("WATCH_TABLE")
    println( "Watch Count:" )
    println(watch_select_df.count())


    Logger.logInfo("Selecting columns from JOIN...")
    val join_data_df = sqlContext.sql(
        """
        select 
            P.PROGRAM_ID,
            P.PARENT_PROGRAM_ID,
            P.MASTER_TITLE,
            P.CATEGORY_ID,
            P.SUBCATEGORY_ID,
            P.RELEASE_YEAR,
            P.SERIES_MASTER_YN,
            P.RUNTIME,
            P.EVENT_DATE,
            P.EPISODE_NUMBER,
            P.EPISODE_TITLE,
            P.PROGRAM_EPISODE_RANK,
            P.PREV_PROGRAM_ID,

            sum(CH.SURFTIME_VIEWER) as NO_OF_SURF_VIEWERS,
            -- sum(case when CH.DURATION > 0.7*P.RUNTIME then 1 else 0 end) as NO_OF_FULL_SHOW_VIEWERS,
            count(distinct CH.SK_DEVICE_ID) as PREV_NO_WATCHERS,
            avg(CH.SURFTIME) as PREV_AVG_SURF_TIME,
            avg(CH.DURATION)/3600 as PREV_AVG_DURATION_HRS

        from 
            PROGRAM_TABLE P
            left join
            WATCH_TABLE CH
            on 
            CH.PROGRAM_ID = P.PREV_PROGRAM_ID
            
        group by 
            P.PROGRAM_ID,
            P.PARENT_PROGRAM_ID,
            P.MASTER_TITLE,
            P.CATEGORY_ID,
            P.SUBCATEGORY_ID,
            P.RELEASE_YEAR,
            P.SERIES_MASTER_YN,
            P.RUNTIME,
            P.EVENT_DATE,
            P.EPISODE_NUMBER,
            P.EPISODE_TITLE,
            P.PROGRAM_EPISODE_RANK,
            P.PREV_PROGRAM_ID            
        """)
    Logger.logInfo("Selecting columns from JOIN...Done")

    println( "Schema of FINAL JOIN Data :" )
    join_data_df.printSchema()
    join_data_df.show()
    // join_data_df.cache()
    // join_data_df.registerTempTable("JOIN_DATA_TABLE") // register as temptable
    println( "JOIN Count:" )
    println(join_data_df.count())


    join_data_df.repartition(1)
      .write
      .mode(saveMode = "Overwrite")
      .option("header", "true")
      .parquet(commonUtils.partitioned_path_channel_data+"parquet/")
    //read_data.write.format.csv("ads.csv")
    //read_data.write.format("com.databricks.spark.csv").save("/user/bda03tivo/ads_feb_test/mydata.csv")
    Logger.logInfo("Stopping SparkContext...")
    sc.stop()
    Logger.logInfo("Stopping SparkContext...Done")
  }
}

