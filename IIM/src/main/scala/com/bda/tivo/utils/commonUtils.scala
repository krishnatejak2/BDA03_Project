package com.bda.tivo.utils

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
object commonUtils{

  val raw_path_channel_data = "/user/bda03tivo/channel/*"
  val raw_path_program_data = "/user/bda03tivo/partitioned/program/CorrectFile2018/parquet/"
  val raw_path_program_data_with_prev_prog = "/user/bda03tivo/partitioned/program/parquetwithprev/"
  val raw_path_campaign_program_data = "/Users/mthota/Dropbox/Data/Data/CAMPAIGN_PROGRAM/parquet/"
  val raw_path_device_data = "/user/bda03tivo/Device/"
  val raw_path_ads_data=""
  val raw_path_campaign_data="/Users/mthota/Dropbox/Data/Data/Campaign/"
  val raw_path_cluster_output_data = "/user/bda03tivo/Output/clusterdata/"

  val partitioned_path_channel_data="/user/bda03tivo/partitioned/channel/"
  val partitioned_path_ads_data=""
  val partitioned_path_program_data="/Users/mthota/Dropbox/Data/Data/Program/CorrectFile2018/"
  val partitioned_path_campaign_program_data="/Users/mthota/Dropbox/Data/Data/Program/CorrectFile2018/"

  val MY_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS"
  val MY_DATETIME_FORMAT: DateTimeFormatter =
    DateTimeFormat.forPattern(MY_DATETIME_PATTERN)

  val dayparts = Array("LATE", "OVERNIGHT", "OVERNIGHT", "OVERNIGHT", "OVERNIGHT", "OVERNIGHT", "MORNING", "MORNING", "MORNING", "MORNING", "MORNING", "DAY", "DAY", "DAY", "DAY", "DAY", "FRINGE", "FRINGE", "FRINGE",
    "PRIME", "PRIME", "PRIME", "PRIME", "LATE")

  def getHouroftheDay(dateString:String): Int ={
    return MY_DATETIME_FORMAT.parseDateTime(dateString).toString("HH").toInt
  }
  def getDayoftheMonth(dateString:String): Int ={
    return MY_DATETIME_FORMAT.parseDateTime(dateString).toString("dd").toInt
  }
  def getMonthoftheYear(dateString:String): Int ={
    return MY_DATETIME_FORMAT.parseDateTime(dateString).toString("MM").toInt
  }
  def getYear(dateString:String): Int ={
    return MY_DATETIME_FORMAT.parseDateTime(dateString).toString("yyyy").toInt
  }

  def getWeekoftheYear(dateString:String):String={

//    val df = new SimpleDateFormat(MY_DATETIME_PATTERN)
//    val date = df.parse(dateString)
//
//    val cal = Calendar.getInstance
//    cal.setTime(date)
//    return  cal.get(Calendar.WEEK_OF_YEAR)
    return MY_DATETIME_FORMAT.parseDateTime(dateString).weekOfWeekyear().getAsText
  }
  def getDayOfWeek(dateString:String):Int={
    return MY_DATETIME_FORMAT.parseDateTime(dateString).getDayOfWeek()

//    val MONDAY = 1
//    val TUESDAY = 2
//    val WEDNESDAY = 3
//    val THURSDAY = 4
//    val FRIDAY = 5
//    val SATURDAY = 6
//    val SUNDAY = 7
  }
  def getDayOfWeekAsText(dateString:String):String={
    return MY_DATETIME_FORMAT.parseDateTime(dateString).dayOfWeek().getAsText()

  }
  def isWeekEnd(dateString:String):Int={
    if( MY_DATETIME_FORMAT.parseDateTime(dateString).getDayOfWeek() > 5) return 1
    return 0
  }
  def isHoliday(dateString:String):String={

    import java.util.Calendar
    val df = new SimpleDateFormat(MY_DATETIME_PATTERN)
    val date = df.parse(dateString)

    val cal = Calendar.getInstance
    cal.setTime(date)


    // check if New Year's Day
    if ((cal.get(Calendar.MONTH) == Calendar.JANUARY) && (cal.get(Calendar.DAY_OF_MONTH) == 1)) return "NEW YEAR"

    // check if Christmas
    if ((cal.get(Calendar.MONTH) == Calendar.DECEMBER) && (cal.get(Calendar.DAY_OF_MONTH) == 25)) return "CHRISTMAS"

    // check if 4th of July
    if ((cal.get(Calendar.MONTH) == Calendar.JULY) && (cal.get(Calendar.DAY_OF_MONTH) == 4)) return "4th JULY"

    // check Thanksgiving (4th Thursday of November)
    if ((cal.get(Calendar.MONTH) == Calendar.NOVEMBER) && (cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 4) && (cal.get(Calendar.DAY_OF_WEEK) == Calendar.THURSDAY)) return "THANKSGIVING"

    // check Halloween (4th Thursday of October)
    if ((cal.get(Calendar.MONTH) == Calendar.OCTOBER) && (cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 4) && (cal.get(Calendar.DAY_OF_WEEK) == Calendar.THURSDAY)) return "HALLOWEEN"

    // check Memorial Day (last Monday of May)
    if ((cal.get(Calendar.MONTH) == Calendar.MAY) && (cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) && cal.get(Calendar.DAY_OF_MONTH) > (31 - 7)) return "MEMORIAL DAY"

    // check Labor Day (1st Monday of September)
    if ((cal.get(Calendar.MONTH) == Calendar.SEPTEMBER) && (cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 1) && (cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY)) return "LABOUR DAY"

    // check President's Day (3rd Monday of February)
    if ((cal.get(Calendar.MONTH) == Calendar.FEBRUARY) && (cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 3) && (cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY)) return "PRESIDENTS DAY"

    // check Veterans Day (November 11)
    if ((cal.get(Calendar.MONTH) == Calendar.NOVEMBER) && (cal.get(Calendar.DAY_OF_MONTH) == 11)) return "VETERANS DAY"

    // check MLK Day (3rd Monday of January)
    if ((cal.get(Calendar.MONTH) == Calendar.JANUARY) && (cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 3) && (cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY)) return "MLK DAY"

    // new Years Eve
    if ((cal.get(Calendar.MONTH) == Calendar.DECEMBER) && (cal.get(Calendar.DAY_OF_MONTH) == 31)) return "NEW YEAR EVE"
    //Christmas Eve
    if ((cal.get(Calendar.MONTH) == Calendar.DECEMBER) && (cal.get(Calendar.DAY_OF_MONTH) == 24)) return "CHRISTMAS EVE"

    // IF NOTHING ELSE, IT'S A BUSINESS DAY
    return "NONE"
  }
  def getDayPart(dateString:String):String={
   return dayparts(getHouroftheDay(dateString))

  }
  def getAirDate(dateString:String):String={
    return dateString.substring(0,10)
  }
  def getEventEndDate(dateString:String,runtime:String):String={
    try{
    MY_DATETIME_FORMAT.parseDateTime(dateString).plusSeconds(runtime.toInt).toString(MY_DATETIME_PATTERN)}
    catch
      {
        case e: Exception =>  DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(dateString).plusSeconds(runtime.toInt).toString("yyyy-MM-dd HH:mm:ss")
      }
  }
  def getAirTime(dateString:String):String={
    return dateString.substring(11,16)
  }
  // Unit testing the code
  def main(args: Array[String]): Unit = {
    val dateString = "2018-01-01 23:30:00.000"
    println("getHouroftheDay: ",getHouroftheDay(dateString))
    println("getDayoftheMonth: ",getDayoftheMonth(dateString))
    println("getDayOfWeek :",getDayOfWeek(dateString))
    println("getDayOfWeekAsText :",getDayOfWeekAsText(dateString))
    println("getMonthoftheYear : ",getMonthoftheYear(dateString))
    println("isHoliday: ",isHoliday(dateString))
    println("isWeekEnd :",isWeekEnd(dateString))
    println("dayPart: ",getDayPart(dateString))
    println("getWeekoftheYear : ",getWeekoftheYear(dateString))
    println("getAirStartDate : ",getAirDate(dateString))
    println("getAirStartTime : ",getAirTime(dateString))
    println("getEventEndDate : ",getEventEndDate(dateString,"3600"))
    println("getEventEndDate : ",getYear(dateString))
  }

}