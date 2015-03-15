package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.{BSEEndOfDayStockPriceFetcher, BSEListOfScripsFetcher}
import com.moneypenny.model._
import com.moneypenny.util.RunableDates
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vives on 3/1/15.
 */
class BSEEndOfDayStockPriceManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseEndOfDayStockPriceFetcher = new BSEEndOfDayStockPriceFetcher

  val bseEndOfDayStockPriceDAO = new BSEEndOfDayStockPriceDAO(context.bseEndOfDayStockPriceCollection)
  val bseEndOfDayStockPriceStatsDAO = new BSEEndOfDayStockPriceStatsDAO(context.bseEndOfDayStockPriceStatsCollection)

  def fetchListOfScrips = {
    val bSEListOfScripsFetcher = new BSEListOfScripsFetcher
    bSEListOfScripsFetcher.fetchListOfScrips
  }

  def getLastRun = {
    bseEndOfDayStockPriceDAO.findLatest
  }

  def parse (data : scala.collection.mutable.Map[(Long, String, String), String]) = {
    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    data map {
      case (key, data) =>
        val (scripCode, scripId, scripName) = key
        logger.debug("BSEEndOfDayStockPrice data as String")
        logger.debug(data)
        val value = CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords.par
        logger.info("Parsing BSEEndOfDayStockPrice Records - " + value.length)
        value map {
          csvRecord =>
            try {
              val dateStr = csvRecord.get("Date") + " 15:45:00"
              Some(BSEEndOfDayStockPrice(BSEEndOfDayStockPriceKey(scripCode, scripId, scripName, dtf.parseLocalDateTime(dateStr).toDate),
                if (csvRecord.get("Open Price").length == 0)  0 else csvRecord.get("Open Price").toDouble,
                if (csvRecord.get("High Price").length == 0)  0 else csvRecord.get("High Price").toDouble,
                if (csvRecord.get("Low Price").length == 0)  0 else csvRecord.get("Low Price").toDouble,
                if (csvRecord.get("Close Price").length == 0)  0 else csvRecord.get("Close Price").toDouble,
                if (csvRecord.get("WAP").length == 0)  0 else csvRecord.get("WAP").toDouble,
                if (csvRecord.get("No.of Shares").length == 0)  0 else csvRecord.get("No.of Shares").toLong,
                if (csvRecord.get("No. of Trades").length == 0)  0 else csvRecord.get("No. of Trades").toLong,
                if (csvRecord.get("Total Turnover (Rs.)").length == 0)  0 else csvRecord.get("Total Turnover (Rs.)").toDouble,
                if (csvRecord.get("Deliverable Quantity").length == 0)  0 else csvRecord.get("Deliverable Quantity").toLong,
                if (csvRecord.get("% Deli. Qty to Traded Qty").length == 0)  0 else csvRecord.get("% Deli. Qty to Traded Qty").toDouble,
                if (csvRecord.get("Spread High-Low").length == 0)  0 else csvRecord.get("Spread High-Low").toDouble,
                if (csvRecord.get("Spread Close-Open").length == 0)  0 else csvRecord.get("Spread Close-Open").toDouble)
              )
            } catch {
              case ex : Exception => logger.info("Exception while parsing BSEEndOfDayStockPrice records for " +
                s"$scripCode, $scripId, $scripName with csvRecord as " + csvRecord, ex)
                None
            }
        } flatten
    } flatMap {
      case ar => {
        logger.debug("Performing flatMap")
        ar.toList
      }
    }
  }

  def fetch (nextRunDate : String, endDate : String) : List[BSEEndOfDayStockPrice] = {
    val data = bseEndOfDayStockPriceFetcher.fetch(nextRunDate, endDate)
    parse(data).toList
  }

  def fetch (startDate : String, endDate : String, scripCode : Long, scripId : String, scripName : String) : List[BSEEndOfDayStockPrice] = {
    val data = bseEndOfDayStockPriceFetcher.fetch(startDate, endDate, scripCode, scripId, scripName)
    parse(data).toList
  }

  def fetch : List[BSEEndOfDayStockPrice] = {
    val lastRunKeys = getLastRun
    val list = if (lastRunKeys.isEmpty) {
      val (startDate, endDate) = RunableDates.getStartAndEndDates(null)
      fetch(startDate, endDate)
    } else {
      val bseListOfScripsKV = fetchListOfScrips map {
        case x : BSEListOfScrips => (x._id.scripCode,
          BSEEndOfDayStockPriceKey(x._id.scripCode, x.scripId.get, x.scripName.get, null))
      }

      val lastRunKeysKV = lastRunKeys map {
        case x : BSEEndOfDayStockPriceKey => (x.scripCode, x)
      }

      (bseListOfScripsKV ++ lastRunKeysKV).groupBy(_._1).map {
        case (key, values) => if (values.length == 1) values(0)._2 else values.filter(_._2.tradeDate != null)(0)._2
      }.toList.par map {
        key => val (startDate, endDate) = RunableDates.getStartAndEndDates(key.tradeDate)
          fetch(startDate, endDate, key.scripCode, key.scripId, key.scripName)
      } reduce(_ ++ _)
    }

    logger.info("Fetched " + list.length + " BSEEndOfDayStockPrice records")
    list
  }

  def insert = {
    val list = fetch
    logger.info("Inserting " + list.length + " BSEEndOfDayStockPrice records")
    list.grouped(10000).foreach {
      case sList => {
        val res1 =  bseEndOfDayStockPriceDAO.bulkUpdate(sList)
        val statsList = sList map {
          case bseEndOfDayStockPrice => {
            BSEEndOfDayStockPriceStats(BSEEndOfDayStockPriceStatsKey(new Date(), bseEndOfDayStockPrice._id), bseEndOfDayStockPrice._id.tradeDate, "OK")
          }
        }
        val res2 = bseEndOfDayStockPriceStatsDAO.bulkInsert(statsList)
        logger.info("Result1 - " + res1)
        logger.info("Result2 - " + res2)
      }
    }
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running BSEEndOfDayStockPriceManager")
      insert
    } catch {
      case ex : Exception => logger.error("Error running BSEEndOfDayStockPriceManager", ex)
    }
  }
}


object BSEEndOfDayStockPriceManagerTest {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

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