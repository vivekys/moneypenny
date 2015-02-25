package com.moneypenny.db

import com.mongodb.ServerAddress
import com.mongodb.casbah.{MongoClient, MongoDB}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

/**
 * Created by vives on 2/8/15.
 */
class MongoContext {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  private val config = ConfigFactory.load

  private var _db: MongoDB = null

  lazy val test_coll = _db("test_coll")

  lazy val bseClientCategorywiseTurnoverCollection = _db("bseClientCategorywiseTurnoverCollection")
  lazy val bseClientCategorywiseTurnoverStatsCollection = _db("bseClientCategorywiseTurnoverStatsCollection")

  lazy val bseCorporateActionCollection = _db("bseCorporateActionCollection")
  lazy val bseCorporateActionStatsCollection = _db("bseCorporateActionStatsCollection")

  lazy val bseEndOfDayStockPriceCollection = _db("bseEndOfDayStockPriceCollection")
  lazy val bseEndOfDayStockPriceStatsCollection = _db("bseEndOfDayStockPriceStatsCollection")

  lazy val bseEquityMarketSummaryCollection = _db("bseEquityMarketSummaryCollection")
  lazy val bseEquityMarketSummaryStatsCollection = _db("bseEquityMarketSummaryStatsCollection")

  lazy val bseIndicesCollection = _db("bseIndicesCollection")
  lazy val bseIndicesStatsCollection = _db("bseIndicesStatsCollection")

  lazy val bseListOfScripsCollection = _db("bseListOfScripsCollection")
  lazy val bseListOfScripsStatsCollection = _db("bseListOfScripsStatsCollection")

  lazy val bseTradingHighlightsCollection = _db("bseTradingHighlightsCollection")
  lazy val bseTradingHighlightsStatsCollection = _db("bseTradingHighlightsStatsCollection")

  lazy val moneycontrolListOfScripsCollection = _db("moneycontrolListOfScripsCollection")
  lazy val moneycontrolListOfScripsStatsCollection = _db("moneycontrolListOfScripsStatsCollection")

  lazy val balanceSheetCollection = _db("balanceSheetCollection")
  lazy val balanceSheetStatsCollection = _db("balanceSheetStatsCollection")

  lazy val profitAndLossCollection = _db("profitAndLossCollection")
  lazy val profitAndLossStatsCollection = _db("profitAndLossStatsCollection")

  lazy val quarterlyResultsCollection = _db("quarterlyResultsCollection")
  lazy val quarterlyResultsStatsCollection = _db("quarterlyResultsStatsCollection")

  lazy val halfYearlyResultsCollection = _db("halfYearlyResultsCollection")
  lazy val halfYearlyResultsStatsCollection = _db("halfYearlyResultsStatsCollection")

  lazy val yearlyResultsCollection = _db("yearlyResultsCollection")
  lazy val yearlyResultsStatsCollection = _db("yearlyResultsStatsCollection")

  lazy val cashFlowCollection = _db("cashFlowCollection")
  lazy val cashFlowStatsCollection = _db("cashFlowStatsCollection")

  lazy val ratiosCollection = _db("ratiosCollection")
  lazy val ratiosStatsCollection = _db("ratiosStatsCollection")


  def connect() {

    val host = config.getString("com.moneypenny.db.host")
    val port = config.getInt("com.moneypenny.db.port")
    val username = config.getString("com.moneypenny.db.username")
    val password = config.getString("com.moneypenny.db.password")
    val dbName = config.getString("com.moneypenny.db.dbName")

//    val credentials = MongoCredential.createCredential(username, dbName, password.toCharArray)

    val server = new ServerAddress("localhost", port)
    val client = MongoClient(server)
    _db = client(dbName)

  }


}
