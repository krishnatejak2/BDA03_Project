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

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors


object UserClusters {

  def main(args: Array[String]) {

  	Logger.logInfo("Successfully created Spark Configuration Object")
	
    /********* SPark Config ********/
    // val conf = new SparkConf().setAppName("channel-data-app")	//new configuration
    val conf = new SparkConf().setAppName("bda03tivo-ClusterApp") //new configuration
	val sc = new SparkContext(conf)	// new SparkContext
	
    Logger.logInfo("Successfully created Spark Context")

	val sqlContext = new org.apache.spark.sql.SQLContext(sc) // SQLContext
    import sqlContext.implicits._ // changes RDD to DF

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


	Logger.logInfo("Joining WATCH and PROGRAM info...")
    val program_watch_data = sqlContext.sql(
        """
        select 
        	W.SK_DEVICE_ID,
        	sum(case when P.SUBCATEGORY_ID = '11' then W.DURATION else 0 end) as 11_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '44' then W.DURATION else 0 end) as 44_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '15' then W.DURATION else 0 end) as 15_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '52' then W.DURATION else 0 end) as 52_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '12' then W.DURATION else 0 end) as 12_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '16' then W.DURATION else 0 end) as 16_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '21' then W.DURATION else 0 end) as 21_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '17' then W.DURATION else 0 end) as 17_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '28' then W.DURATION else 0 end) as 28_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '54' then W.DURATION else 0 end) as 54_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '45' then W.DURATION else 0 end) as 45_DURATION,
        	sum(case when P.SUBCATEGORY_ID = '23' then W.DURATION else 0 end) as 23_DURATION,
        	sum(case when P.SUBCATEGORY_ID not in ('11','44','15','52','12','16','21','17','28','54','45','23')then W.DURATION else 0 end) as ALL_ELSE_DURATION
        from 
        	WATCH W            
            left join 
            PROGRAM P
            on W.PROGRAM_ID = P.PROGRAM_ID
        where
        	P.CATEGORY_ID in ('5','3','8')
 		group by 
 			W.SK_DEVICE_ID
        """)
    Logger.logInfo("Joining WATCH and PROGRAM info...Done")

    println( "Schema of PROGRAM_WATCH : After joining PROGRAM & WATCH info" )
    program_watch_data.printSchema()   
    program_watch_data.show()

    println("PROGRAM_WATCH Count:" )
    println(program_watch_data.count())    

    program_watch_data.repartition(1)
      .write
      .mode(saveMode = "Overwrite")
      .option("header", "true")
      .parquet(commonUtils.raw_path_cluster_output_data)

	}
}