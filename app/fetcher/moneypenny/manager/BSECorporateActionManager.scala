package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSECorporateActionFetcher
import com.moneypenny.model._
import com.moneypenny.util.RunableDates
import org.apache.commons.csv.{CSVRecord, CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.parallel.immutable.ParSeq

/**
 * Created by vives on 3/1/15.
 */
class BSECorporateActionManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseCorporateActionFetcher = new BSECorporateActionFetcher

  val bseCorporateActionDAO = new BSECorporateActionDAO(context.bseCorporateActionCollection)
  val bseCorporateActionStatsDAO = new BSECorporateActionStatsDAO(context.bseCorporateActionStatsCollection)

  def parse (data : Map[(Long, String, String), String]) = {
    val dtf = DateTimeFormat.forPattern("dd MMM yyyy")
    data filter (rec => rec._2.length != 0) map {
      case (key, value) => {
        val (scripCode, scripId, scripName) = key
        val records = try {
          CSVParser.parse(value, CSVFormat.EXCEL.withHeader().withIgnoreSurroundingSpaces(true)).getRecords.par
        } catch {
          case ex : Exception => logger.info("Exception while parsing BSECorporateAction records for " +
            s"$scripCode, $scripId, $scripName with data as " + value, ex)
            ParSeq.empty[CSVRecord]
        }
        records map {
          case csvRecord => try {
            Some(BSECorporateAction(BSECorporateActionKey(scripCode, scripName,
              if (csvRecord.get("Ex Date").length == 0 || csvRecord.get("Ex Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Ex Date")).toDate),
              if (csvRecord.get("Purpose").length == 0) None else Some(csvRecord.get("Purpose")),
              if (csvRecord.get("Record Date").length == 0 || csvRecord.get("Record Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Record Date")).toDate),
              if (csvRecord.get("BC Start Date").length == 0 || csvRecord.get("BC Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC Start Date")).toDate),
              if (csvRecord.get("BC End Date").length == 0 || csvRecord.get("BC End Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC End Date")).toDate),
              if (csvRecord.get("ND Start Date").length == 0 || csvRecord.get("ND Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND Start Date")).toDate),
              if (csvRecord.get("ND End Date").length == 0 || csvRecord.get("ND End Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND End Date")).toDate),
              if (csvRecord.get("Actual Payment Date").length == 0 || csvRecord.get("Actual Payment Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Actual Payment Date")).toDate))
            ))
          } catch {
            case ex : Exception => logger.info("Exception while parsing BSECorporateAction records for " +
              s"$scripCode, $scripId, $scripName with csvRecord as " + csvRecord, ex)
              None
          }
        }
      } flatten
    } flatMap {
      case ar => ar.toList
    }
  }

  def fetch(nextRunDate : String, endDate : String) : List[BSECorporateAction] = {
    val data = bseCorporateActionFetcher.fetch(nextRunDate, endDate)
    parse(data.toMap).toList
  }

  def fetch : List[BSECorporateAction] = {
    val (startDate, endDate) = RunableDates.getStartAndEndDates(null)

    val list = fetch(startDate, endDate)
    logger.info("Fetched " + list.length + " BSECorporateAction records")
    list
  }

  def insert = {
    val list = fetch
    logger.info("Inserting " + list.length + " BSECorporateAction records")
    val res1 = bseCorporateActionDAO.bulkUpdate(list)
    val currentDate = new Date()
    val statsList = list.par map {
      case bseCorporateAction => {
        new BSECorporateActionStats(new BSECorporateActionStatsKey(currentDate, bseCorporateAction._id), new Date(), "OK")
      }
    }
    val res2 = bseCorporateActionStatsDAO.bulkUpdate(statsList.toList)
    logger.info("Result1 - " + res1 + " Result2 - " + res2)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running BSECorporateActionManager")
      insert
    } catch {
      case ex : Exception => logger.error("Error running BSECorporateActionManager", ex)
    }
  }

}


object BSECorporateActionManagerTest {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val job = JobBuilder.newJob(classOf[BSECorporateActionManager])
      .withIdentity("BSECorporateActionManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("BSECorporateActionManagerTrigger", "MoneyPennyFetcher"))
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