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

  lazy val bseEndOfDayStockPriceCollection = _db("bseEndOfDayStockPriceCollection")
  lazy val bseIndicesCollection = _db("bseIndicesCollection")
  lazy val bseCorporateActionCollection = _db("bseCorporateActionCollection")
  lazy val bseClientCategorywiseTurnoverCollection = _db("bseClientCategorywiseTurnoverCollection")
  lazy val bseEquityMarketSummaryCollection = _db("bseEquityMarketSummaryCollection")
  lazy val bseListOfScripsCollection = _db("bseListOfScripsCollection")
  lazy val bseTradingHighlightsCollection = _db("bseTradingHighlightsCollection")
  lazy val moneycontrolListOfScripsCollection = _db("moneycontrolListOfScripsCollection")

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
