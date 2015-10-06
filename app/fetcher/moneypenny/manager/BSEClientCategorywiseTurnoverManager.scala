package com.moneypenny.manager


import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.BSEClientCategorywiseTurnoverFetcher
import com.moneypenny.model._
import com.moneypenny.util.RunableDates
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vives on 2/23/15.
 */
class BSEClientCategorywiseTurnoverManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val bseClientCategorywiseTurnoverFetcher = new BSEClientCategorywiseTurnoverFetcher

  val bseClientCategorywiseTurnoverDAO = new BSEClientCategorywiseTurnoverDAO(context.bseClientCategorywiseTurnoverCollection)
  val bseClientCategorywiseTurnoverStatsDAO = new BSEClientCategorywiseTurnoverStatsDAO(context.bseClientCategorywiseTurnoverStatsCollection)

  def parse (data : String) : Iterable[BSEClientCategorywiseTurnover] = {
    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords.par map {
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
    } toList
  }

  def fetch(nextRunDate : String, endDate : String) : List[BSEClientCategorywiseTurnover] = {
    val data = bseClientCategorywiseTurnoverFetcher.fetch(nextRunDate, endDate)
    parse(data).toList
  }

  def fetch : List[BSEClientCategorywiseTurnover] = {
    val (startDate, endDate) = RunableDates.getStartAndEndDates(null)
    
    val list = fetch(startDate, endDate)
    logger.info("Fetched " + list.length + " BSEClientCategorywiseTurnover records")
    list
  }

  def insert = {
    val list = fetch
    logger.info("Inserting " + list.length + " BSEClientCategorywiseTurnover records")
    val res1 = bseClientCategorywiseTurnoverDAO.bulkUpdate(list)

    val statsList = list.par map {
      case bseClientCategorywiseTurnover => {
        new BSEClientCategorywiseTurnoverStats(
        new BSEClientCategorywiseTurnoverStatsKey(new Date(), bseClientCategorywiseTurnover._id),
        bseClientCategorywiseTurnover._id.tradeDate, "OK")
      }
    }
    val res2 = bseClientCategorywiseTurnoverStatsDAO.bulkInsert(statsList.toList)
    logger.info("Result1 - " + res1 + " Result2 - " + res2)

  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running BSEClientCategorywiseTurnoverManager")
      insert
    } catch {
      case ex : Exception => logger.error("Error running BSEClientCategorywiseTurnoverManager", ex)
    }
  }

}


object BSEClientCategorywiseTurnoverManagerTest {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

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