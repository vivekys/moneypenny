package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSEIndicesFetcher
import com.moneypenny.model._
import com.moneypenny.util.RunableDates
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

import scala.collection.JavaConversions._

/**
 * Created by vives on 2/17/15.
 */
class BSEIndicesManager extends Job {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseIndicesFetcher = new BSEIndicesFetcher

  val bseIndicesDAO = new BSEIndicesDAO(context.bseIndicesCollection)
  val bseIndicesStatsDAO = new BSEIndicesStatsDAO(context.bseIndicesStatsCollection)

  def getLastRun = {
    bseIndicesStatsDAO.findLatest
  }

  def parseBSEIndicesFetcherData (indices : Map[String, String]) : Iterable[BSEIndices] = {
    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    indices map {
      case (index, data) => CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
        csvRecord =>
          val dateStr = csvRecord.get("Date") + " 15:45:00"
          val bseIndicesKey = new BSEIndicesKey(index, dtf.parseLocalDateTime(dateStr).toDate)
          val bseIndices = new BSEIndices(bseIndicesKey,
            if (csvRecord.get("Open").length == 0)  0 else csvRecord.get("Open").toDouble,
            if (csvRecord.get("High").length == 0)  0 else csvRecord.get("High").toDouble,
            if (csvRecord.get("Low").length == 0)  0 else csvRecord.get("Low").toDouble,
            if (csvRecord.get("Close").length == 0)  0 else csvRecord.get("Close").toDouble)
          bseIndices
      }
    } flatMap {
      case ar => ar.toList
    }
  }

  def fetchBseIndices(nextRunDate : String, endDate : String) : List[BSEIndices] = {
    val data = bseIndicesFetcher.fetch(nextRunDate, endDate)
    parseBSEIndicesFetcherData(data).toList
  }

  def fetchBseIndices : List[BSEIndices] = {
    val lastRunStat = getLastRun
    val (startDate, endDate) = RunableDates.getStartAndEndDates(lastRunStat match {
      case Some(x) => x.lastRun
      case None => null
    })

    val list = fetchBseIndices(startDate, endDate)
    logger.info("Fetched " + list.length + " BSEIndices records")
    list
  }

  def insertBSEIndices = {
    val list = fetchBseIndices
    logger.info("Inserting " + list.length + " bseIndices records")
    val res1 = bseIndicesDAO.bulkUpdate(list)
    val statsList = list map {
      case bseIndices => {
        new BSEIndicesStats(new BSEIndicesStatsKey(new Date(), bseIndices._id), bseIndices._id.tradeDate, "OK")
      }
    }
    val res2 = bseIndicesStatsDAO.bulkInsert(statsList)
    logger.info("Result1 - " + res1 + " Result2 - " + res2)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    logger.info("Running BSEIndicesManager")
    insertBSEIndices
  }

}


object BSEIndicesManagerTest {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val job = JobBuilder.newJob(classOf[BSEIndicesManager])
                    .withIdentity("BSEIndicesManager", "MoneyPennyFetcher")
                    .build()

    val trigger = TriggerBuilder.newTrigger()
                            .withIdentity(new TriggerKey("BSEIndicesManagerTrigger", "MoneyPennyFetcher"))
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