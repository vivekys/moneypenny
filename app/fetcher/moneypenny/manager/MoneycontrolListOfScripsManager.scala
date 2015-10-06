package com.moneypenny.manager

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.MoneycontrolStockListFetcher
import com.moneypenny.model._
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.parallel.mutable.ParIterable

/**
 * Created by vives on 3/6/15.
 */
class MoneycontrolListOfScripsManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val listOfScripsFetcher = MoneycontrolStockListFetcher

  val moneycontrolListOfScripsDAO = new MoneycontrolListOfScripsDAO(context.moneycontrolListOfScripsCollection)

  def parse (listOfScrips: ParIterable[scala.collection.mutable.Map[String, Map[String, Option[String]]]]) : Iterable[MoneycontrolListOfScrips] = {
    logger.info("Parsing data for MoneycontrolListOfScrips. Num of Records : " + listOfScrips.size)
    listOfScrips.toList.filterNot(_.isEmpty) map {
      case data => data map {
        case (name, metaData) =>
          val ele =
            MoneycontrolListOfScrips(MoneycontrolListOfScripsKey(name, metaData.get("url").get.get),
              metaData.get("BSE") match {
              case None => None
              case Some(x) => x match {
                case None => None
                case Some(str) => Some(str.toLong)
              }
            }, metaData.get("NSE").getOrElse(None), metaData.get("SECTOR").getOrElse(None))
          ele
      }
    } flatMap {
      case ar => ar.toList
    }
  }


  def fetch = {
    val listOfScrips = parse(MoneycontrolStockListFetcher.fetch).toList
    logger.info("Fetched " + listOfScrips.length + " MoneycontrolListOfScrips records")
    listOfScrips
  }

  def insert = {
    val list = fetch
    logger.info("Inserting " + list.length + " MoneycontrolListOfScrips records")
    val res1 = moneycontrolListOfScripsDAO.bulkUpdate(list)
    logger.info("Result1 - " + res1)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running MoneycontrolListOfScripsManager")
      insert
    } catch {
      case ex : Exception => logger.error("Error running MoneycontrolListOfScripsManager", ex)
    }
  }
}


object MoneycontrolListOfScripsManagerTest {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val job = JobBuilder.newJob(classOf[MoneycontrolListOfScripsManager])
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