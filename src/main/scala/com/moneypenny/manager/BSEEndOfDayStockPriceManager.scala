package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSEEndOfDayStockPriceFetcher
import com.moneypenny.model._
import com.moneypenny.util.RunableDates
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

import scala.collection.JavaConversions._

/**
 * Created by vives on 3/1/15.
 */
class BSEEndOfDayStockPriceManager extends Job {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseEndOfDayStockPriceFetcher = new BSEEndOfDayStockPriceFetcher

  val bseEndOfDayStockPriceDAO = new BSEEndOfDayStockPriceDAO(context.bseEndOfDayStockPriceCollection)
  val bseEndOfDayStockPriceStatsDAO = new BSEEndOfDayStockPriceStatsDAO(context.bseEndOfDayStockPriceStatsCollection)

  def getLastRun = {
    bseEndOfDayStockPriceStatsDAO.findLatest
  }

  def parseBSEEndOfDayStockPriceFetcherData (data : Map[(Long, String, String), String]) = {
    data map {
      case (key, data) =>
        val (scripCode, scripId, scripName) = key
        CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
          csvRecord =>
            val dateStr = csvRecord.get("Date") + " 15:45:00"
            BSEEndOfDayStockPrice(BSEEndOfDayStockPriceKey(scripCode, scripId, scripName, dtf.parseLocalDateTime(dateStr).toDate),
              if (csvRecord.get("Open Price").length == 0)  0 else csvRecord.get("Open Price").toDouble,
              if (csvRecord.get("High Price").length == 0)  0 else csvRecord.get("High Price").toDouble,
              if (csvRecord.get("Low Price").length == 0)  0 else csvRecord.get("Low Price").toDouble,
              if (csvRecord.get("Close Price").length == 0)  0 else csvRecord.get("Close Price").toDouble,
              if (csvRecord.get("WAP").length == 0)  0 else csvRecord.get("WAP").toDouble,
              if (csvRecord.get("No.of Shares").length == 0)  0 else csvRecord.get("No.of Shares").toLong,
              if (csvRecord.get("No. of Trades").length == 0)  0 else csvRecord.get("No. of Trades").toLong,
              if (csvRecord.get("Total Turnover (Rs.)").length == 0)  0 else csvRecord.get("Total Turnover (Rs.)").toLong,
              if (csvRecord.get("Deliverable Quantity").length == 0)  0 else csvRecord.get("Deliverable Quantity").toLong,
              if (csvRecord.get("% Deli. Qty to Traded Qty").length == 0)  0 else csvRecord.get("% Deli. Qty to Traded Qty").toDouble,
              if (csvRecord.get("Spread High-Low").length == 0)  0 else csvRecord.get("Spread High-Low").toDouble,
              if (csvRecord.get("Spread Close-Open").length == 0)  0 else csvRecord.get("Spread Close-Open").toDouble
            )
        }
    } flatMap {
      case ar => ar.toList
    }
  }

  def fetchBSEEndOfDayStockPrice(nextRunDate : String, endDate : String) : List[BSEEndOfDayStockPrice] = {
    val data = bseEndOfDayStockPriceFetcher.fetch(nextRunDate, endDate)
    parseBSEEndOfDayStockPriceFetcherData(data.toMap).toList
  }

  def fetchBSEEndOfDayStockPrice : List[BSEEndOfDayStockPrice] = {
    val lastRunStat = getLastRun
    val (startDate, endDate) = RunableDates.getStartAndEndDates(lastRunStat match {
      case Some(x) => x.lastRun
      case None => null
    })

    val list = fetchBSEEndOfDayStockPrice(startDate, endDate)
    logger.info("Fetched " + list.length + " BSEEndOfDayStockPrice records")
    list
  }

  def insertBSEEndOfDayStockPrice = {
    val list = fetchBSEEndOfDayStockPrice
    logger.info("Inserting " + list.length + " BSEEndOfDayStockPrice records")
    val res1 = bseEndOfDayStockPriceDAO.bulkUpdate(list)
    val statsList = list map {
      case bseEndOfDayStockPrice => {
        BSEEndOfDayStockPriceStats(BSEEndOfDayStockPriceStatsKey(new Date(), bseEndOfDayStockPrice._id), bseEndOfDayStockPrice._id.tradeDate, "OK")
      }
    }
    val res2 = bseEndOfDayStockPriceStatsDAO.bulkInsert(statsList)
    logger.info("Result1 - " + res1 + " Result2 - " + res2)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    logger.info("Running BSEIndicesManager")
    insertBSEEndOfDayStockPrice
  }
}


object BSEEndOfDayStockPriceManagerTest {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val job = JobBuilder.newJob(classOf[BSEEndOfDayStockPriceManager])
      .withIdentity("BSEEndOfDayStockPriceManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("BSEEndOfDayStockPriceManagerTrigger", "MoneyPennyFetcher"))
      .startNow()
      .withSchedule(SimpleScheduleBuilder.simpleSchedule()
      .withIntervalInHours(24)
      .withRepeatCount(1))
      .build()

    val schedularFactory = new StdSchedulerFactory
    val sched = schedularFactory.getScheduler

    sched.scheduleJob(job, trigger)
    sched.start

    Thread.sleep(90000000L * 1000L)
    sched.shutdown(true)
  }
}