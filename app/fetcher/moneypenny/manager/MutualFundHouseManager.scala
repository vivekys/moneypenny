package fetcher.moneypenny.manager

import com.moneypenny.db.MongoContext
import com.moneypenny.util.RetryFunExecutor
import fetcher.moneypenny.fetcher._
import fetcher.moneypenny.model._
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

/**
 * Created by vivek on 10/10/15.
 */
class MutualFundHouseManager extends Job {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val context = new MongoContext
  context.connect()

  val mfHouseFetcher = new MutualFundHouseFetcher

  val mfDAO = new MutualFundHouseDAO(context.mfFundHouseCollection)

  def parse (mfHouses : Map[String, String]) = {
    logger.info("Parsing MutualFundHouse Names Map of Size - " + mfHouses.size)
    mfHouses map {
      mfHouse => MutualFundHouse(MutualFundHouseKey(mfHouse._1), mfHouse._2)
    } toList
  }

  def insert : Unit = {
    val mfHouses = parse(mfHouseFetcher.getMFHouses)
    logger.info("Inserting " + mfHouses.size + " parsed MutualFundHouse Names")
    val res0 = mfDAO.bulkUpdate(mfHouses)
    logger.info("getInsertedCount - " + res0.getInsertedCount)
    logger.info("getMatchedCount - " + res0.getMatchedCount)
    logger.info("getModifiedCount - " + res0.getModifiedCount)
    logger.info("getRemovedCount - " + res0.getRemovedCount)
    logger.info("getUpserts - " + res0.getUpserts.size)
  }

  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    try {
      logger.info("Running MutualFundHouseManager")
      RetryFunExecutor.retry(3) {
        insert
      }
      logger.info("Completed MutualFundHouseManager")
    } catch {
      case ex : Exception => logger.error("Error running MutualFundHouseManager", ex)
    }
  }
}

object MutualFundHouseManagerTest {
  def main (args: Array[String]) {

    val job = JobBuilder.newJob(classOf[MutualFundHouseManager])
      .withIdentity("MutualFundHouseManager", "MoneyPennyFetcher")
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(new TriggerKey("MutualFundHouseManagerTrigger", "MoneyPennyFetcher"))
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