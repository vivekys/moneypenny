package fetcher.moneypenny.manager

import com.moneypenny.db.MongoContext
import com.moneypenny.fetcher.MoneycontrolFinancialFetcher
import com.moneypenny.model._
import fetcher.moneypenny.fetcher.FinancialType
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.mutable.LinkedHashMap

/**
 * Created by vivek on 06/04/15.
 */
class FinancialsManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  val moneycontrolFinancialFetcher = new MoneycontrolFinancialFetcher
  val BalanceSheetDAO = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.BalanceSheet.toString))
  val PnLDAO = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.PnL.toString))
  val QuarterlyResultsDAO = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.QuarterlyResults.toString))
  val HalfYearlyResultsDAO = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.HalfYearlyResults.toString))
  val NineMonthlyResultsDAO = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.NineMonthlyResults.toString))
  val YearlyResultsDAO = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.YearlyResults.toString))
  val CashFlowDAO = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.CashFlow.toString))
  val Ratios = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(FinancialType.Ratios.toString))

  def fetchListOfScrips = {
    val context = new MongoContext
    context.connect()

    val dao = new MoneycontrolListOfScripsDAO(context.moneycontrolListOfScripsCollection)
    dao.findAll
  }

  def parseAndInsertToDB (name : String, data : LinkedHashMap[String, LinkedHashMap[String, Map[String, Option[Number]]]]) = {
    logger.info(s"Parsing the financial data for $name")

    val dtf = DateTimeFormat.forPattern("MMM ''yy dd HH:mm:ss")

    data map {
      case (fyType, fyData) => {
        val Array(fin, finType) = fyType.split("-")
        logger.info(s"Parsing the data for $fin")
        val dao = new FinancialsDAO(FinancialsDAOUtil.getFinancialCollection(fin))
        val financialList = scala.collection.mutable.MutableList.empty[Financials]
        fyData map {
          case (fyDateStr, fyMap) => {
            val fyDate = dtf.parseLocalDateTime(fyDateStr + " 01 15:45:00").dayOfMonth().withMaximumValue.toDate
            logger.info(s"Parsing $fin - $finType for the financial year $fyDate")
            val financials = Financials(FinancialsKey(name, fin, finType, fyDate), fyMap)
            financialList += financials
          }
        }
        val res = dao.bulkUpdate(financialList.toList)
        logger.info("Result - " + res)
      }
    }

  }

  def fetchAndProcess = {
    val scripList = fetchListOfScrips
    for (scrip <- scripList) {
      val data = moneycontrolFinancialFetcher.fetchAllFinancialData(scrip._id.url)
      parseAndInsertToDB(scrip._id.companyName ,data)
    }
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running FinancialsManager")
      fetchAndProcess
    } catch {
      case ex : Exception => logger.error("Error running FinancialsManager", ex)
    }
  }
}

object FinancialsManagerTest {
  def test {
    val mcBalSheetFetcher = new MoneycontrolFinancialFetcher
    val returnMap = mcBalSheetFetcher.fetchAllFinancialData("http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01")
    val financialsManager = new FinancialsManager
    financialsManager.parseAndInsertToDB("akcapitalservices", returnMap)
  }

  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

//    val job = JobBuilder.newJob(classOf[FinancialsManager])
//      .withIdentity("FinancialsManager", "FinancialsFetcher")
//      .build()
//
//    val trigger = TriggerBuilder.newTrigger()
//      .withIdentity(new TriggerKey("FinancialsManagerTrigger", "FinancialsFetcher"))
//      .startNow()
//      .withSchedule(SimpleScheduleBuilder.simpleSchedule()
//      .withIntervalInHours(24)
//      .withRepeatCount(1))
//      .build()
//
//    val schedularFactory = new StdSchedulerFactory
//    val sched = schedularFactory.getScheduler
//
//    sched.scheduleJob(job, trigger)
//    sched.start
//
//    Thread.sleep(90000000L * 1000L)
//    sched.shutdown(true)
    test
  }
}
