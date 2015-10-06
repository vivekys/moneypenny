package fetcher.moneypenny.manager

import java.util.Calendar

import com.moneypenny.db.MongoContext
import fetcher.moneypenny.fetcher.YahooListOfScripsFetcher
import fetcher.moneypenny.model._
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory



/**
 * Created by vivek on 04/10/15.
 */
class YahooListOfScripsManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val yahooListOfScripsFetcher = new YahooListOfScripsFetcher

  val yahooListOfScripsDAO = new YahooListOfScripsDAO(context.yahooListOfScripsCollection)

  val yahooListOfScripsStatsDAO = new YahooListOfScripsStatsDAO(context.yahooListOfScripsStatsCollection)

  def parse (data : Map[String, (String, String)]) = {
    data map {
      case (symbol, (scripName, exchange)) => {
        logger.info(s"Parsing YahooListOfScrips for $symbol")
        YahooListOfScrips(YahooListOfScripsKey(symbol), scripName, exchange)
      }
    } toList
  }

  def insert: Unit = {
    val char = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
      "T", "U", "V", "W", "X", "Y", "Z", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0")

    char.map( letter => {
      val parsedData = parse(yahooListOfScripsFetcher.fetchFor(letter))
      yahooListOfScripsDAO.bulkUpdate(parsedData)
      yahooListOfScripsStatsDAO.insert(YahooListOfScripsStats(YahooListOfScripsStatsKey(Calendar.getInstance().getTime), letter))
    })

  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running YahooListOfScripsManager")
      insert
      logger.info("Completed YahooListOfScripsManager")
    } catch {
      case ex : Exception => logger.error("Error running YahooListOfScripsManager", ex)
    }
  }
}

object YahooListOfScripsManagerTest {
  def main (args: Array[String]) {

    val job = JobBuilder.newJob(classOf[YahooListOfScripsManager])
      .withIdentity("YahooListOfScripsManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("YahooListOfScripsManagerTrigger", "MoneyPennyFetcher"))
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