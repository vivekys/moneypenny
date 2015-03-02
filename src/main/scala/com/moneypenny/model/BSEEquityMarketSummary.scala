package com.moneypenny.model

import java.util.Date

import com.moneypenny.db.MongoContext
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._
import org.joda.time.format.DateTimeFormat

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/markets/Equity/EQReports/Historical_EquitySegment.aspx?expandable=7
case class BSEEquityMarketSummaryKey (date : Date)

case class BSEEquityMarketSummary(_id : BSEEquityMarketSummaryKey, numCompaniesTraded : Long, numTrades : Long,
                                  numShares : Long, netTurnOver : Double)


object BSEEquityMarketSummaryMap {
  def toBson(bseEquityMarketSummary : BSEEquityMarketSummary) = {
    grater[BSEEquityMarketSummary].asDBObject(bseEquityMarketSummary)
  }

  def fromBsom(o: DBObject) : BSEEquityMarketSummary = {
    grater[BSEEquityMarketSummary].asObject(o)
  }
}

class BSEEquityMarketSummaryDAO (collection : MongoCollection) {
  def insert(bseEquityMarketSummary : BSEEquityMarketSummary) = {
    val doc = BSEEquityMarketSummaryMap.toBson(bseEquityMarketSummary)
    collection.insert(doc)
  }

  def bulkInsert (bseEquityMarketSummaryList : List[BSEEquityMarketSummary]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseEquityMarketSummaryList map {
      case bseEquityMarketSummary => builder.insert(BSEEquityMarketSummaryMap.toBson(bseEquityMarketSummary))
    }
    builder.execute()
  }

  def update(bseEquityMarketSummary : BSEEquityMarketSummary) = {
    val query = MongoDBObject("_id.date" -> bseEquityMarketSummary._id.date)
    val doc = BSEEquityMarketSummaryMap.toBson(bseEquityMarketSummary)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (bseEquityMarketSummaryList : List[BSEEquityMarketSummary]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseEquityMarketSummaryList map {
      case bseEquityMarketSummary => builder.find(MongoDBObject("_id.date" -> bseEquityMarketSummary._id.date)).
        upsert().update(
          new BasicDBObject("$set",BSEEquityMarketSummaryMap.toBson(bseEquityMarketSummary)))
    }
    builder.execute()
  }


  def findOne (key : BSEEquityMarketSummaryKey) : Option[BSEEquityMarketSummary] = {
    val doc = collection.findOne(MongoDBObject("_id.date" -> key.date)).getOrElse(return None)
    Some(BSEEquityMarketSummaryMap.fromBsom(doc))
  }
}


object BSEEquityMarketSummaryDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    val tradeDate = dtf.parseLocalDateTime("28-January-2015 15:45:00")

    val key = BSEEquityMarketSummaryKey(tradeDate.toDate)
    val bseEquityMarketSummary = BSEEquityMarketSummary(key,3106,3074438, 324369103, 30377540314.00)

    val context = new MongoContext
    context.connect()

    val dao = new BSEEquityMarketSummaryDAO(context.test_coll)

    dao.insert(bseEquityMarketSummary)

    println(dao.findOne(key))
    val bseEquityMarketSummaryUpdated = BSEEquityMarketSummary(key,3106, 0, 324369103, 30377540314.00)
    dao.bulkUpdate(List(bseEquityMarketSummaryUpdated))
    println(dao.findOne(key))
  }
}

