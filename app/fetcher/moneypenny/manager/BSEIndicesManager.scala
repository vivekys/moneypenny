package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSEIndicesFetcher
import com.moneypenny.model._
import com.moneypenny.util.RunableDates
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vives on 2/17/15.
 */
class BSEIndicesManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseIndicesFetcher = new BSEIndicesFetcher

  val bseIndicesDAO = new BSEIndicesDAO(context.bseIndicesCollection)
  val bseIndicesStatsDAO = new BSEIndicesStatsDAO(context.bseIndicesStatsCollection)

  def getLastRun = {
    bseIndicesDAO.findLatest
  }

  def parse (indices : Map[(String, String), String]) : Iterable[BSEIndices] = {
    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    indices map {
      case ((indexId, indexName), data) => CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
        csvRecord => try {
          val dateStr = csvRecord.get("Date") + " 15:45:00"
          val bseIndicesKey = new BSEIndicesKey(indexId, indexName, dtf.parseLocalDateTime(dateStr).toDate)
          val bseIndices = new BSEIndices(bseIndicesKey,
            if (csvRecord.get("Open").length == 0)  0 else csvRecord.get("Open").toDouble,
            if (csvRecord.get("High").length == 0)  0 else csvRecord.get("High").toDouble,
            if (csvRecord.get("Low").length == 0)  0 else csvRecord.get("Low").toDouble,
            if (csvRecord.get("Close").length == 0)  0 else csvRecord.get("Close").toDouble)
          Some(bseIndices)
        } catch {
          case ex : Exception => logger.info("Exception while parsing BSEIndices records for " +
            "csvRecord " + csvRecord, ex)
            None
        }
      } flatten
    } flatMap {
      case ar => ar.toList
    }
  }

  def fetch (nextRunDate : String, endDate : String) : List[BSEIndices] = {
    val data = bseIndicesFetcher.fetch(nextRunDate, endDate)
    parse(data).toList
  }

  def fetch (nextRunDate : String, endDate : String, indexId : String) : List[BSEIndices] = {
    val data = bseIndicesFetcher.fetch(nextRunDate, endDate, indexId)
    parse(data).toList
  }

  def fetch : List[BSEIndices] = {
    val lastRunKeys = getLastRun
    val list = if (lastRunKeys.isEmpty) {
                  val (startDate, endDate) = RunableDates.getStartAndEndDates(null)
                  fetch(startDate, endDate)
    } else {
      lastRunKeys.toList.par map {
        key => val (startDate, endDate) = RunableDates.getStartAndEndDates(key.tradeDate)
               fetch(startDate, endDate, key.indexId)
        } reduce(_ ++ _)
    }

    logger.info("Fetched " + list.length + " BSEIndices records")
    list
  }

  def insert = {
    val list = fetch
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
    try {
      logger.info("Running BSEIndicesManager")
      insert
    } catch {
      case ex : Exception => logger.error("Error running BSEIndicesManager", ex)
    }
  }
}


object BSEIndicesManagerTest {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

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