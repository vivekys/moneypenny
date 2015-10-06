package fetcher.moneypenny.manager

import java.util.{Date, Calendar}

import com.moneypenny.db.MongoContext
import fetcher.moneypenny.fetcher.YahooAdjustedClosePriceFetcher
import fetcher.moneypenny.model._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.quartz.impl.StdSchedulerFactory
import org.quartz._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vivek on 05/10/15.
 */
class YahooAdjustedClosePriceManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val yahooAdjustedClosePriceFetcher = new YahooAdjustedClosePriceFetcher

  val yahooAdjustedClosePriceDAO = new YahooAdjustedClosePriceDAO(context.yahooAdjustedClosePriceCollection)

  val yahooAdjustedClosePriceStatsDAO = new YahooAdjustedClosePriceStatsDAO(context.yahooAdjustedClosePriceStatsCollection)

  def parse (data : String, symbol : String, scripName : String, exchange : String) = {
    val dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
      case csvRecord => {
        val dateStr = csvRecord.get("Date") + " 15:45:00"
        YahooAdjustedClosePrice(YahooAdjustedClosePriceKey(symbol,
          dtf.parseDateTime(dateStr).toDate),
          scripName, exchange,
          if (csvRecord.get("Open").length == 0)  0 else csvRecord.get("Open").toDouble,
          if (csvRecord.get("High").length == 0)  0 else csvRecord.get("High").toDouble,
          if (csvRecord.get("Low").length == 0)  0 else csvRecord.get("Low").toDouble,
          if (csvRecord.get("Close").length == 0)  0 else csvRecord.get("Close").toDouble,
          if (csvRecord.get("Volume").length == 0)  0 else csvRecord.get("Volume").toLong,
          if (csvRecord.get("Adj Close").length == 0)  0 else csvRecord.get("Adj Close").toDouble
        )
      }
    } toList
  }

  def findScripsToFetch = {
    val dao = new YahooListOfScripsDAO(context.yahooListOfScripsCollection)
    val scrips = dao.findAll
    scrips
  }

  def insert: Unit = {
    val scrips = findScripsToFetch
    scrips map (yahooListOfScrips => {
      val data = yahooAdjustedClosePriceFetcher.fetchFor(
        yahooListOfScrips._id.symbol, yahooListOfScrips.scripName,
        yahooListOfScrips.exchange)
      data match {
        case Some(d) => {
          val yahooAdjustedClosePriceList = parse(d, yahooListOfScrips._id.symbol, yahooListOfScrips.scripName,
            yahooListOfScrips.exchange)

          logger.info("Inserting yahooAdjustedClosePrice " + yahooAdjustedClosePriceList.size +
            " records for " + yahooListOfScrips.scripName)

          val res0 = yahooAdjustedClosePriceDAO.bulkUpdate(yahooAdjustedClosePriceList)
          val res1 = yahooAdjustedClosePriceStatsDAO.insert(YahooAdjustedClosePriceStats(
            YahooAdjustedClosePriceStatsKey(Calendar.getInstance().getTime, yahooListOfScrips.scripName)))
          logger.info("getInsertedCount - " + res0.getInsertedCount)
          logger.info("getMatchedCount - " + res0.getMatchedCount)
          logger.info("getModifiedCount - " + res0.getModifiedCount)
          logger.info("getRemovedCount - " + res0.getRemovedCount)
          logger.info("getUpserts - " + res0.getUpserts.size)
        }
        case None => logger.info("No Data for " + yahooListOfScrips.scripName)
      }
    })
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running YahooAdjustedClosePriceManager")
      insert
      logger.info("Completed YahooAdjustedClosePriceManager")
    } catch {
      case ex : Exception => logger.error("Error running YahooAdjustedClosePriceManager", ex)
    }
  }

}

object YahooAdjustedClosePriceManagerTest {
  def main (args: Array[String]) {

    val job = JobBuilder.newJob(classOf[YahooAdjustedClosePriceManager])
      .withIdentity("YahooAdjustedClosePriceManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("YahooAdjustedClosePriceManagerTrigger", "MoneyPennyFetcher"))
      .startNow()
      .withSchedule(SimpleScheduleBuilder.simpleSchedule()
        .withIntervalInHours(24)
        .withRepeatCount(1))
      .build()

    val schedulerFactory = new StdSchedulerFactory
    val sched = schedulerFactory.getScheduler

    sched.scheduleJob(job, trigger)
    sched.start

    Thread.sleep(90000000L * 1000L)
    sched.shutdown(true)


  }
}