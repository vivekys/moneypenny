package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSECorporateActionFetcher
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
class BSECorporateActionManager extends Job {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseCorporateActionFetcher = new BSECorporateActionFetcher

  val bseCorporateActionDAO = new BSECorporateActionDAO(context.bseCorporateActionCollection)
  val bseCorporateActionStatsDAO = new BSECorporateActionStatsDAO(context.bseCorporateActionStatsCollection)

  def getLastRun = {
    bseCorporateActionStatsDAO.findLatest
  }

  def parseBSECorporateActionFetcherData (data : Map[(Long, String, String), String]) = {
    val dtf = DateTimeFormat.forPattern("dd MMM yyyy")
    data map {
      case (key, value) => {
        val (scripCode, scripId, scripName) = key
        CSVParser.parse(value, CSVFormat.EXCEL.withHeader()).getRecords map {
          case csvRecord => BSECorporateAction(BSECorporateActionKey(scripCode, scripId, scripName,
            if (csvRecord.get("Ex Date").length == 0 || csvRecord.get("Ex Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Ex Date")).toDate),
            if (csvRecord.get("Purpose").length == 0) None else Some(csvRecord.get("Purpose")),
            if (csvRecord.get("Record Date").length == 0 || csvRecord.get("Record Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Record Date")).toDate),
            if (csvRecord.get("BC Start Date").length == 0 || csvRecord.get("BC Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC Start Date")).toDate),
            if (csvRecord.get("BC End Date	").length == 0 || csvRecord.get("BC End Date	").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC End Date	")).toDate),
            if (csvRecord.get("ND Start Date").length == 0 || csvRecord.get("ND Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND Start Date")).toDate),
            if (csvRecord.get("ND End Date").length == 0 || csvRecord.get("ND End Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND End Date")).toDate),
            if (csvRecord.get("Actual Payment Date").length == 0 || csvRecord.get("Actual Payment Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Actual Payment Date")).toDate)))
        }
      }
    } flatMap {
      case ar => ar.toList
    }
  }

  def fetchBSECorporateAction(nextRunDate : String, endDate : String) : List[BSECorporateAction] = {
    val data = bseCorporateActionFetcher.fetch(nextRunDate, endDate)
    parseBSECorporateActionFetcherData(data.toMap).toList
  }

  def fetchBSECorporateAction : List[BSECorporateAction] = {
    val lastRunStat = getLastRun
    val (startDate, endDate) = RunableDates.getStartAndEndDates(lastRunStat match {
      case Some(x) => x.lastRun
      case None => null
    })

    val list = fetchBSECorporateAction(startDate, endDate)
    logger.info("Fetched " + list.length + " BSECorporateAction records")
    list
  }

  def insertBSECorporateAction = {
    val list = fetchBSECorporateAction
    logger.info("Inserting " + list.length + " BSECorporateAction records")
    val res1 = bseCorporateActionDAO.bulkUpdate(list)
    val statsList = list map {
      case bseCorporateAction => {
        new BSECorporateActionStats(new BSECorporateActionStatsKey(new Date(), bseCorporateAction._id), new Date(), "OK")
      }
    }
    val res2 = bseCorporateActionStatsDAO.bulkInsert(statsList)
    logger.info("Result1 - " + res1 + " Result2 - " + res2)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    logger.info("Running BSECorporateActionManager")
    insertBSECorporateAction
  }

}


object BSECorporateActionManagerTest {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

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