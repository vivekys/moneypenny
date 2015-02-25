package com.moneypenny.manager


import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSEClientCategorywiseTurnoverFetcher
import com.moneypenny.model._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.{LocalTime, LocalDateTime, LocalDate}
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import scala.collection.JavaConversions._

/**
 * Created by vives on 2/23/15.
 */
class BSEClientCategorywiseTurnoverManager extends Job {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseClientCategorywiseTurnoverFetcher = new BSEClientCategorywiseTurnoverFetcher

  val bseClientCategorywiseTurnoverDAO = new BSEClientCategorywiseTurnoverDAO(context.bseClientCategorywiseTurnoverCollection)
  val bseClientCategorywiseTurnoverStatsDAO = new BSEClientCategorywiseTurnoverStatsDAO(context.bseClientCategorywiseTurnoverStatsCollection)

  def getLastRun = {
    bseClientCategorywiseTurnoverStatsDAO.findLatest
  }

  def getCurrentRunnableDate (stat : Option[BSEClientCategorywiseTurnoverStats]) = {
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


  def parseBSEClientCategorywiseTurnoverData (data : String) : Iterable[BSEClientCategorywiseTurnover] = {
    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
      case csvRecord => {
        val dateStr = csvRecord.get("Trade Date") + " 15:45:00"
        val bseClientCategorywiseTurnoverKey = new BSEClientCategorywiseTurnoverKey(dtf.parseLocalDateTime(dateStr).toDate)
        val bseClientCategorywiseTurnover = new BSEClientCategorywiseTurnover(bseClientCategorywiseTurnoverKey,
          if (csvRecord.get("Clients Buy").length == 0)  0 else csvRecord.get("Clients Buy").toDouble,
          if (csvRecord.get("Clients Sales").length == 0)  0 else csvRecord.get("Clients Sales").toDouble,
          if (csvRecord.get("Clients Net").length == 0)  0 else csvRecord.get("Clients Net").toDouble,
          if (csvRecord.get("NRI Buy").length == 0)  0 else csvRecord.get("NRI Buy").toDouble,
          if (csvRecord.get("NRI Sales").length == 0)  0 else csvRecord.get("NRI Sales").toDouble,
          if (csvRecord.get("NRI Net").length == 0)  0 else csvRecord.get("NRI Net").toDouble,
          if (csvRecord.get("Proprietary Buy").length == 0)  0 else csvRecord.get("Proprietary Buy").toDouble,
          if (csvRecord.get("Proprietary Sales").length == 0)  0 else csvRecord.get("Proprietary Sales").toDouble,
          if (csvRecord.get("Proprietary Net").length == 0)  0 else csvRecord.get("Proprietary Net").toDouble,
          if (csvRecord.get("IFIs Buy").length == 0)  0 else csvRecord.get("IFIs Buy").toDouble,
          if (csvRecord.get("IFIs Sales").length == 0)  0 else csvRecord.get("IFIs Sales").toDouble,
          if (csvRecord.get("IFIs Net").length == 0)  0 else csvRecord.get("IFIs Net").toDouble,
          if (csvRecord.get("Banks Buy").length == 0)  0 else csvRecord.get("Banks Buy").toDouble,
          if (csvRecord.get("Banks Sales").length == 0)  0 else csvRecord.get("Banks Sales").toDouble,
          if (csvRecord.get("Banks Net").length == 0)  0 else csvRecord.get("Banks Net").toDouble,
          if (csvRecord.get("Insurance Buy").length == 0)  0 else csvRecord.get("Insurance Buy").toDouble,
          if (csvRecord.get("Insurance Sales").length == 0)  0 else csvRecord.get("Insurance Sales").toDouble,
          if (csvRecord.get("Insurance Net").length == 0)  0 else csvRecord.get("Insurance Net").toDouble,
          if (csvRecord.get("DII(BSE + NSE + MCX-SX) Buy").length == 0)  0 else csvRecord.get("DII(BSE + NSE + MCX-SX) Buy").toDouble,
          if (csvRecord.get("DII(BSE + NSE + MCX-SX) Sales").length == 0)  0 else csvRecord.get("DII(BSE + NSE + MCX-SX) Sales").toDouble,
          if (csvRecord.get("DII(BSE + NSE + MCX-SX) Net").length == 0)  0 else csvRecord.get("DII(BSE + NSE + MCX-SX) Net").toDouble)
        bseClientCategorywiseTurnover
      }
    }
  }

  def fetchBSEClientCategorywiseTurnover(nextRunDate : String, endDate : String) : List[BSEClientCategorywiseTurnover] = {
    val data = bseClientCategorywiseTurnoverFetcher.fetch(nextRunDate, endDate)
    parseBSEClientCategorywiseTurnoverData(data).toList
  }

  def fetchBSEClientCategorywiseTurnover : List[BSEClientCategorywiseTurnover] = {
    val nextRunDate = "01/01/1990"

    //TODO Add config here
    val endDate = getEndDate

    val list = fetchBSEClientCategorywiseTurnover(nextRunDate, endDate)
    logger.info("Fetched " + list.length + " BSEClientCategorywiseTurnover records")
    list
  }

  def insertBSEClientCategorywiseTurnoverManager = {
    var count = 0
    val list = fetchBSEClientCategorywiseTurnover
    logger.info("Inserting " + list.length + " BSEClientCategorywiseTurnover records")
    list map {
      case bseClientCategorywiseTurnover => {
        count += 1
        logger.info("Inserting BSEClientCategorywiseTurnover - " + bseClientCategorywiseTurnover._id.tradeDate)
        val res1 = bseClientCategorywiseTurnoverDAO.update(bseClientCategorywiseTurnover)
        val res2 = bseClientCategorywiseTurnoverStatsDAO.insert(new BSEClientCategorywiseTurnoverStats(
                                      new BSEClientCategorywiseTurnoverStatsKey(new Date(), bseClientCategorywiseTurnover._id),
                                      bseClientCategorywiseTurnover._id.tradeDate, "OK"))
        logger.info("Result1 - " + res1 + " Result2 - " + res2)
      }
    }
    logger.info("Inserted " + count + " BSEClientCategorywiseTurnover records")
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    logger.info("Running BSEClientCategorywiseTurnoverManager")
    insertBSEClientCategorywiseTurnoverManager
  }

}


object BSEClientCategorywiseTurnoverManagerTest {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val job = JobBuilder.newJob(classOf[BSEClientCategorywiseTurnoverManager])
      .withIdentity("BSEClientCategorywiseTurnoverManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("BSEClientCategorywiseTurnoverTrigger", "MoneyPennyFetcher"))
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