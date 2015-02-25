package com.moneypenny.manager

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSEIndicesFetcher
import com.moneypenny.model._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.{LocalTime, LocalDateTime, LocalDate}
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

  def getCurrentRunnableDate (stat : Option[BSEIndicesStats]) = {
    val dateFormatToRun = DateTimeFormat.forPattern("dd/MM/YYYY")
    val currentDateTime = new LocalDateTime(new Date())
    stat match {
      case Some(s) => {
        val localDate = new LocalDate(s.lastRun)
        val nextRunDate = if (localDate.plusDays(1).toLocalDateTime(new LocalTime(18, 0, 0)).compareTo(currentDateTime) < 0)
          localDate.plusDays(1)
        else
          currentDateTime.toLocalDate
        logger.info("Last Run - " + dateFormatToRun.print(localDate) +
          " and Next Run - " + dateFormatToRun.print(nextRunDate))
        dateFormatToRun.print(nextRunDate)
      }
      case None => {
        logger.info("Last Run - None" +
          " and Next Run - 01/01/1990")
        "01/01/1990"
      }
    }
  }

  def getEndDate = {
    val dateFormatToRun = DateTimeFormat.forPattern("dd/MM/YYYY")
    val currentDateTime = new LocalDateTime(new Date())

    if (currentDateTime.getHourOfDay >= 18)
      dateFormatToRun.print(currentDateTime)
    else
      dateFormatToRun.print(currentDateTime.minusDays(1))
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
    val nextRunDate = getCurrentRunnableDate(lastRunStat)

    val dateFormatToRun = DateTimeFormat.forPattern("dd/MM/YYYY")
    val currentDateTime = new LocalDateTime(new Date())


    //TODO Add config here
    val endDate = getEndDate

    val list = fetchBseIndices(nextRunDate, endDate)
    logger.info("Fetched " + list.length + " BSEIndices records")
    list
  }

  def insertBSEIndices = {
    var count = 0
    val list = fetchBseIndices
    logger.info("Inserting " + list.length + " bseIndices records")
    list map {
      case bseIndices => {
        count += 1
        logger.info("Inserting bseIndices - " + bseIndices._id.index + " for the date - " + bseIndices._id.date)
        val res1 = bseIndicesDAO.insert(bseIndices)
        val res2 = bseIndicesStatsDAO.insert(new BSEIndicesStats(new BSEIndicesStatsKey(new Date(), bseIndices._id), bseIndices._id.date, "OK"))
        logger.info("Result1 - " + res1 + " Result2 - " + res2)
      }
    }
    logger.info("Inserted " + count + " bseIndices records")
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

//    val job = JobBuilder.newJob(classOf[BSEIndicesManager])
//                    .withIdentity("BSEIndicesManager", "MoneyPennyFetcher")
//                    .build()
//
//    val trigger = TriggerBuilder.newTrigger()
//                            .withIdentity(new TriggerKey("BSEIndicesManagerTrigger", "MoneyPennyFetcher"))
//                            .startNow()
//                            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
//                                            .withIntervalInHours(24)
//                                            .withRepeatCount(1))
//                            .build()
//
//    val schedularFactory = new StdSchedulerFactory
//    val sched = schedularFactory.getScheduler
//
//    sched.scheduleJob(job, trigger)
//    sched.start
//
//    Thread.sleep(90000000L * 1000L)
//    sched.shutdown(true)
    val bseIndicesManager = new BSEIndicesManager
    bseIndicesManager.insertBSEIndices
  }
}