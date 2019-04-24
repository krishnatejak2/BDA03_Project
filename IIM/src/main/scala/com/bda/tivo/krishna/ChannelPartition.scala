package com.bda.tivo.krishna

import com.bda.tivo.utils.commonUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf

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
    
    Logger.logInfo("Reading the PROGRAM file...")
        val raw_program_data = sqlContext.read.parquet(commonUtils.raw_path_program_data)
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

    sqlContext.udf.register("add_seconds", (datetime : java.sql.Timestamp, seconds : Int) => {new java.sql.Timestamp(datetime.getTime() + seconds*1000 )
    });

    Logger.logInfo("Selecting columns from Program...")
    val program_select_df = sqlContext.sql(
        """
        select 
            *,
            add_seconds(PROGRAM_STARTTIME,RUNTIME) as PROGRAM_ENDTIME 
        from 
        (
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
                cast(
                    case when P.ORIGINAL_ADT in ('NULL') then 
                        (case when P.EVENT_DATE in ('NULL') then null else P.EVENT_DATE end)
                    else P.ORIGINAL_ADT end as TIMESTAMP
                ) as PROGRAM_STARTTIME
            from 
                PROGRAM P 
            where
                EVENT_DATE <> 'NULL' and EVENT_DATE >= '2018-01-01'
                
        ) X
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
            Y.*,
            case when Y.TIME_ZONE <> 'EST' then add_seconds(Y.WATCHEVENT_STARTTIME,3600) else Y.WATCHEVENT_STARTTIME end as WATCHEVENT_STARTTIME_EST,
            case when Y.TIME_ZONE <> 'EST' then add_seconds(Y.WATCHEVENT_ENDTIME,3600) else Y.WATCHEVENT_ENDTIME end as WATCHEVENT_ENDTIME_EST
        from
        (
             select 
                X.SK_DEVICE_ID,
                X.EVENT_DATE,
                X.EVENT_TIME,
                X.SK_DAYPART_ID,
                X.SESSION_ID,
                X.EVENT_TYPE,
                X.SOURCE_ID,
                X.PROGRAM_ID,
                X.CHANNEL_NUMBER,
                X.BACKGROUND_TUNER,
                X.GUIDE_INFLUENCED,
                X.SEARCH_INFLUENCED,
                X.DURATION,
                X.DURATION/3600 DURATION_HRS,
                X.DURATION/60 DURATION_MIN,
                X.PREV_SESSION_ID,
                X.PARENT_SESSION_ID,
                X.SURFTIME,
                X.SK_LOAD_ID,
                X.FIRST_USER_ACTION,
                X.SURFTIME_VIEWER,
                X.TIME_ZONE,
                cast(concat_ws( ' ', cast(X.EVENT_DATE_DATEFORMAT as string), TIME_CONCAT) as timestamp) as WATCHEVENT_STARTTIME,
                add_seconds(cast(concat_ws( ' ', cast(X.EVENT_DATE_DATEFORMAT as string), TIME_CONCAT) as timestamp),DURATION) WATCHEVENT_ENDTIME 
            from
            (
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
            ) X
        ) Y
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
            P.PROGRAM_STARTTIME,
            P.PROGRAM_ENDTIME,

            sum(CH.SURFTIME_VIEWER) as NO_OF_SURF_VIEWERS,
            sum(case when CH.DURATION > 0.7*P.RUNTIME then 1 else 0 end) as NO_OF_FULL_SHOW_VIEWERS,
            count(distinct CH.SK_DEVICE_ID) as NO_WATCHERS,
            avg(CH.SURFTIME) as AVG_SURF_TIME,
            -- min(CH.EVENT_DATE) as MIN_EVENT_DATE,
            -- max(CH.EVENT_DATE) as MAX_EVENT_DATE,
            avg(CH.DURATION)/3600 as AVG_DURATION_HRS

        from 
            PROGRAM_TABLE P
            join
            WATCH_TABLE CH
            on 
            P.PROGRAM_ID = CH.PROGRAM_ID
            and
            CH.WATCHEVENT_STARTTIME_EST between P.PROGRAM_STARTTIME and P.PROGRAM_ENDTIME 
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
            P.PROGRAM_STARTTIME,
            P.PROGRAM_ENDTIME
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

