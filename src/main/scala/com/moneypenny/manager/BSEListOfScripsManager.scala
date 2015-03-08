package com.moneypenny.manager

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSEListOfScripsFetcher
import com.moneypenny.model._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import scala.collection.JavaConversions._

/**
 * Created by vives on 3/4/15.
 */
class BSEListOfScripsManager extends Job {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseListOfScripsFetcher = new BSEListOfScripsFetcher

  val bseListOfScripsDAO = new BSEListOfScripsDAO(context.bseListOfScripsCollection)

  def parseBSEIndicesFetcherData (bseListOfScrips: String) : Iterable[BSEListOfScrips] = {
    CSVParser.parse(bseListOfScrips, CSVFormat.EXCEL.withHeader()).getRecords map {
      case csvRecords => {
        BSEListOfScrips(BSEListOfScripsKey(csvRecords.get("Scrip Code").toLong),
          if (csvRecords.get("Scrip Id").length == 0) None else Some(csvRecords.get("Scrip Id")),
          if (csvRecords.get("Scrip Name").length == 0) None else Some(csvRecords.get("Scrip Name")),
          if (csvRecords.get("Status").length == 0) None else Some(csvRecords.get("Status")),
          if (csvRecords.get("Group").length == 0) None else Some(csvRecords.get("Group")),
          if (csvRecords.get("Face Value").length == 0) None else Some(csvRecords.get("Face Value").toDouble),
          if (csvRecords.get("ISIN No").length == 0) None else Some(csvRecords.get("ISIN No")),
          if (csvRecords.get("Industry").length == 0) None else Some(csvRecords.get("Industry")),
          if (csvRecords.get("Instrument").length == 0) None else Some(csvRecords.get("Instrument")))
      }
    }
  }

  def fetch : List[BSEListOfScrips] = {
    val bseListOfScrips = parseBSEIndicesFetcherData(bseListOfScripsFetcher.fetch).toList
    logger.info("Fetched " + bseListOfScrips.length + " BSEListOfScrips records")
    bseListOfScrips
  }

  def insert = {
    val list = fetch
    logger.info("Inserting " + list.length + " BSEListOfScrips records")
    val res1 = bseListOfScripsDAO.bulkUpdate(list)
    logger.info("Result1 - " + res1)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running BSEListOfScripsManager")
      insert
    } catch {
      case ex : Exception => logger.error("Error running BSEListOfScripsManager", ex)
    }
  }

}


object BSEListOfScripsManagerTest {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val job = JobBuilder.newJob(classOf[BSEListOfScripsManager])
      .withIdentity("BSEListOfScripsManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("BSEListOfScripsManagerTrigger", "MoneyPennyFetcher"))
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