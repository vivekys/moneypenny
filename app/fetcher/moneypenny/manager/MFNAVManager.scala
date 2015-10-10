package fetcher.moneypenny.manager

import com.moneypenny.db.MongoContext
import fetcher.moneypenny.fetcher.{MFHistoricNAVFetcher, MFNAVFetcher}
import fetcher.moneypenny.manager.MFNAVManagerMode.MFNAVManagerMode
import fetcher.moneypenny.model._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vivek on 10/10/15.
 */

object MFNAVManagerMode extends Enumeration {
  type MFNAVManagerMode = Value
  val CURRENT, HISTORIC = Value
}

class MFNAVManager extends Job {
  var mode : MFNAVManagerMode = MFNAVManagerMode.CURRENT
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val mfHouseDAO = new MutualFundHouseDAO(context.mfFundHouseCollection)
  val mfNAVDAO = new MutualFundNAVDAO(context.mfNAVCollection)
  val mfNAVStatsDAO = new MutualFundNAVStatsDAO(context.mfNAVStatsCollection)

  def parse (data : String) = {
    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    (CSVParser.parse(data, CSVFormat.newFormat(';').withHeader()).getRecords map {
      case csvRecord => {
        try {
          val dateStr = csvRecord.get("Date") + " 15:45:00"
          Some(MutualFundNAV(MutualFundNAVKey(csvRecord.get("Scheme Code").toLong, dtf.parseDateTime(dateStr).toDate),
            csvRecord.get("ISIN Div Payout/ ISIN Growth"),
            csvRecord.get("ISIN Div Reinvestment"),
            csvRecord.get("Scheme Name"),
            csvRecord.get("Net Asset Value").toDouble,
            csvRecord.get("Repurchase Price").toDouble,
            csvRecord.get("Sale Price").toDouble))
        } catch {
          case ex : Exception => {
            logger.info("Failed to Parse " + csvRecord, ex)
            None
          }
        }
      }
    } toList) flatten
  }

  def insert (navList : List[MutualFundNAV]) = {
    logger.info("Inserting " + navList.size + " MF NAV")

    val res = mfNAVDAO.bulkUpdate(navList)
    logger.info("getInsertedCount - " + res.getInsertedCount)
    logger.info("getMatchedCount - " + res.getMatchedCount)
    logger.info("getModifiedCount - " + res.getModifiedCount)
    logger.info("getRemovedCount - " + res.getRemovedCount)
    logger.info("getUpserts - " + res.getUpserts.size)
  }

  def fetchNavAndInsert = {
    mode match {
      case MFNAVManagerMode.CURRENT => {
        val fetcher = new MFNAVFetcher
        val data = fetcher.getNAV
        val navList = parse(data.getOrElse(""))
        insert(navList)
      }
      case MFNAVManagerMode.HISTORIC => {
        val fetcher = new MFHistoricNAVFetcher
        val allFundHouses = mfHouseDAO.findAll
        val tpArr = Array("1", "2", "3")
        tpArr map {
          tp => {
            allFundHouses map {
              fundHouse => {
                val data = fetcher.getNAV(fundHouse._id.keyName, tp)
                val navList = parse(data.getOrElse(""))
                insert(navList)
                val res = mfNAVStatsDAO.insert(MutualFundNAVStats(MutualFundNAVStatsKey(fundHouse._id, tp,
                  java.util.Calendar.getInstance.getTime)))
                logger.info("Inserted  MutualFundNAVStats" + res.toString)
              }
            }
          }
        }
      }
    }
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running MFNAVManager")
      mode = jobExecutionContext.getJobDetail.getJobDataMap.get("mode").asInstanceOf[MFNAVManagerMode]
      fetchNavAndInsert
      logger.info("Completed MFNAVManager")
    } catch {
      case ex : Exception => logger.error("Error running MFNAVManager", ex)
    }
  }
}

object MFNAVManagerTest {
  def main (args: Array[String]) {

    val job = JobBuilder.newJob(classOf[MFNAVManager])
      .withIdentity("MFNAVManager", "MoneyPennyFetcher")
      .build()

    job.getJobDataMap.put("mode", MFNAVManagerMode.HISTORIC)

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("MFNAVManagerTrigger", "MoneyPennyFetcher"))
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