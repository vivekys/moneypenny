package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSETradingHighlightsFetcher
import com.moneypenny.model._
import com.moneypenny.util.RunableDates
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vives on 3/6/15.
 */
class BSETradingHighlightsManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseTradingHighlightsFetcher = new BSETradingHighlightsFetcher

  val bseTradingHighlightsDAO = new BSETradingHighlightsDAO(context.bseTradingHighlightsCollection)
  val bseTradingHighlightsStatsDAO = new BSETradingHighlightsStatsDAO(context.bseTradingHighlightsStatsCollection)

  def getLastRun = {
    bseTradingHighlightsStatsDAO.findLatest
  }

  def parse (data : String) : Iterable[BSETradingHighlights] = {
    val dtf = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
      csvRecord =>
        val dateStr = csvRecord.get("Date") + " 15:45:00"
        BSETradingHighlights(BSETradingHighlightsKey(dtf.parseLocalDateTime(dateStr).toDate),
          if (csvRecord.get("Scrips Traded	").length == 0 || csvRecord.get("Scrips Traded	") == "-")  0 else csvRecord.get("Scrips Traded	").toLong,
          if (csvRecord.get("No. of Trades").length == 0 || csvRecord.get("No. of Trades") == "-")  0 else csvRecord.get("No. of Trades").toLong,
          if (csvRecord.get("Traded Qty.(Cr.)").length == 0 || csvRecord.get("Traded Qty.(Cr.)") == "-")  0 else csvRecord.get("Traded Qty.(Cr.)").toDouble,
          if (csvRecord.get("Total T/O (Cr.)").length == 0 || csvRecord.get("Total T/O (Cr.)") == "-")  0 else csvRecord.get("Total T/O (Cr.)").toDouble,
          if (csvRecord.get("Advance").length == 0 || csvRecord.get("Advance") == "-")  0 else csvRecord.get("Advance").toLong,
          if (csvRecord.get("Advances as % of Scrips Traded").length == 0 || csvRecord.get("Advances as % of Scrips Traded") == "-")  0 else csvRecord.get("Advances as % of Scrips Traded").toDouble,
          if (csvRecord.get("Decline").length == 0 || csvRecord.get("Decline") == "-")  0 else csvRecord.get("Decline").toLong,
          if (csvRecord.get("Declines as % of Scrips Traded	").length == 0 || csvRecord.get("Declines as % of Scrips Traded	") == "-")  0 else csvRecord.get("Declines as % of Scrips Traded	").toDouble,
          if (csvRecord.get("Unchanged").length == 0 || csvRecord.get("Unchanged") == "-")  0 else csvRecord.get("Unchanged").toLong,
          if (csvRecord.get("Unchanged as % of Scrips Traded	").length == 0 || csvRecord.get("Unchanged as % of Scrips Traded	") == "-")  0 else csvRecord.get("Unchanged as % of Scrips Traded	").toDouble,
          if (csvRecord.get("Scrips on Upper Circuit").length == 0 || csvRecord.get("Scrips on Upper Circuit") == "-")  0 else csvRecord.get("Scrips on Upper Circuit").toLong,
          if (csvRecord.get("Scrips on Lower Circuit").length == 0 || csvRecord.get("Scrips on Lower Circuit") == "-")  0 else csvRecord.get("Scrips on Lower Circuit").toLong,
          if (csvRecord.get("Scrips Touching 52W H").length == 0 || csvRecord.get("Scrips Touching 52W H") == "-")  0 else csvRecord.get("Scrips Touching 52W H").toLong,
          if (csvRecord.get("Scrips Touching 52W L").length == 0 || csvRecord.get("Scrips Touching 52W L") == "-")  0 else csvRecord.get("Scrips Touching 52W L").toLong)
    }
  }

  def fetch (nextRunDate : String, endDate : String) : List[BSETradingHighlights] = {
    val data = bseTradingHighlightsFetcher.fetch(nextRunDate, endDate)
    parse(data).toList
  }

  def fetch : List[BSETradingHighlights] = {
    val (startDate, endDate) = RunableDates.getStartAndEndDates(null)

    val list = fetch(startDate, endDate)
    logger.info("Fetched " + list.length + " BSETradingHighlights records")
    list
  }

  def insert = {
    val list = fetch
    logger.info("Inserting " + list.length + " BSETradingHighlights records")
    val res1 = bseTradingHighlightsDAO.bulkUpdate(list)
    val statsList = list map {
      case bseTradingHighlights => {
        new BSETradingHighlightsStats(BSETradingHighlightsStatsKey(new Date(), bseTradingHighlights._id), bseTradingHighlights._id.tradeDate, "OK")
      }
    }
    val res2 = bseTradingHighlightsStatsDAO.bulkInsert(statsList)
    logger.info("Result1 - " + res1 + " Result2 - " + res2)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    logger.info("Running BSETradingHighlightsManager")
    try {
      insert
    } catch {
      case ex : Exception => logger.info("Error running BSETradingHighlightsManager", ex)
    }

  }

}


object BSETradingHighlightsManagerTest {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val job = JobBuilder.newJob(classOf[BSETradingHighlightsManager])
      .withIdentity("BSETradingHighlightsManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("BSETradingHighlightsManagerTrigger", "MoneyPennyFetcher"))
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