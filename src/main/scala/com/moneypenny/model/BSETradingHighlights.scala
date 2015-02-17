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
//http://www.bseindia.com/markets/Equity/EQReports/Tradinghighlights_histroical.aspx?expandable=7
case class BSETradingHighlightsKey(date : Date)

case class BSETradingHighlights(_id : BSETradingHighlightsKey,
                                scripsTraded : Long,
                                numOfTrades : Long,
                                tradedQtyInCr : Double,
                                totalTurnOverInCr : Double,
                                advance : Long,
                                advancesAsPercentOfScripsTraded : Double,
                                decline : Long,
                                declinesAsPercentOfScripsTraded : Double,
                                unchanged : Long,
                                unchangedAsPercentOfScripsTraded : Double,
                                scripsOnUpperCircuit : Long,
                                scripsOnLowerCircuit : Long,
                                scripsTouching52WH : Long,
                                scripsTouching52WL : Long)

object BSETradingHighlightsMap {
  def toBson(bseTradingHighlights : BSETradingHighlights) = {
    grater[BSETradingHighlights].asDBObject(bseTradingHighlights)
  }

  def fromBsom(o: DBObject) : BSETradingHighlights = {
    grater[BSETradingHighlights].asObject(o)
  }
}

class BSETradingHighlightsDAO (collection : MongoCollection) {
  def insert(bseTradingHighlights : BSETradingHighlights) = {
    val doc = BSETradingHighlightsMap.toBson(bseTradingHighlights)
    collection.insert(doc)
  }

  def update(bseTradingHighlights : BSETradingHighlights) = {
    val query = MongoDBObject("_id.date" -> bseTradingHighlights._id.date)
    val doc = BSETradingHighlightsMap.toBson(bseTradingHighlights)
    collection.update(query, doc, upsert=true)
  }

  def findOne (key : BSETradingHighlightsKey) : Option[BSETradingHighlights] = {
    val doc = collection.findOne(MongoDBObject("_id.date" -> key.date,
      "_id.date" -> key.date)).getOrElse(return None)
    Some(BSETradingHighlightsMap.fromBsom(doc))
  }
}

object BSETradingHighlightsDAOManagerTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()
    RegisterJodaLocalDateTimeConversionHelpers

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    val localDate = dtf.parseLocalDateTime("1-January-2015 15:45:00")

    val key = BSETradingHighlightsKey(localDate.toDate)
    val bseTradingHighlights = BSETradingHighlights(key, 3060,2323583,22.41,1934.25,1886,0.62,1057,0.35,117,0.04,205,291,134,61)

    val context = new MongoContext
    context.connect()

    val dao = new BSETradingHighlightsDAO(context.test_coll)

    dao.insert(bseTradingHighlights)

    println(dao.findOne(key))
    val bseTradingHighlightsUpdated = BSETradingHighlights(key, 0,2323583,22.41,1934.25,1886,0.62,1057,0.35,117,0.04,205,291,134,61)
    dao.update(bseTradingHighlightsUpdated)
    println(dao.findOne(key))

  }
}

