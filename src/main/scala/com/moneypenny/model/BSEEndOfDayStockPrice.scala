package com.moneypenny.model

import com.moneypenny.db.MongoContext
import com.moneypenny.util.CaseClassToMapImplicits
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/download/BhavCopy/Equity/EQ281114_CSV.ZIP
case class BSEEndOfDayStockPriceKey (scripCode : Long, scripId : String, scripName : String, date : String)

case class BSEEndOfDayStockPrice(_id: BSEEndOfDayStockPriceKey, openPrice : Double,
                                 highPrice : Double,
                                 lowPrice : Double,
                                 closePrice : Double,
                                 WAP : Double,
                                 numShares : Long,
                                 numTrades : Long,
                                 totalTurnoverInRS : Double,
                                 deliverableQuantity : Long,
                                 perDeliQtyToTradedQty : Double,
                                 highLowDiff : Double,
                                 closeOpenDiff : Double)

object BSEEndOfDayStockPriceMap {

  def toBson(bseEndOfDayStockPrice : BSEEndOfDayStockPrice) = {
    grater[BSEEndOfDayStockPrice].asDBObject(bseEndOfDayStockPrice)
  }

  def fromBsom(o: DBObject) : BSEEndOfDayStockPrice = {
    grater[BSEEndOfDayStockPrice].asObject(o)
  }

}

class BSEEndOfDayStockPriceDAO (collection : MongoCollection) {
  def insert(bseEndOfDayStockPrice : BSEEndOfDayStockPrice) = {
    val doc = BSEEndOfDayStockPriceMap.toBson(bseEndOfDayStockPrice)
    collection.insert(doc)
  }

  def bulkInsert (bseEndOfDayStockPriceList : List[BSEEndOfDayStockPrice]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseEndOfDayStockPriceList map {
      case bseEndOfDayStockPrice => builder.insert(BSEEndOfDayStockPriceMap.toBson(bseEndOfDayStockPrice))
    }
    builder.execute()
  }

  def update(bseEndOfDayStockPrice : BSEEndOfDayStockPrice) = {
    val query = MongoDBObject("_id.scripCode" -> bseEndOfDayStockPrice._id.scripCode,
      "_id.scripId" -> bseEndOfDayStockPrice._id.scripId,
      "_id.scripName" -> bseEndOfDayStockPrice._id.scripName,
      "_id.date" -> bseEndOfDayStockPrice._id.date)
    val doc = BSEEndOfDayStockPriceMap.toBson(bseEndOfDayStockPrice)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (bseEndOfDayStockPriceList : List[BSEEndOfDayStockPrice]) = {
    import CaseClassToMapImplicits._
    val builder = collection.initializeOrderedBulkOperation
    bseEndOfDayStockPriceList map {
      case bseEndOfDayStockPrice => builder.find(MongoDBObject("_id.scripCode" -> bseEndOfDayStockPrice._id.scripCode,
        "_id.scripId" -> bseEndOfDayStockPrice._id.scripId,
        "_id.scripName" -> bseEndOfDayStockPrice._id.scripName,
        "_id.date" -> bseEndOfDayStockPrice._id.date)).upsert().replaceOne(
          MongoDBObject(bseEndOfDayStockPrice.toStringWithFields.filterKeys(_ != "_id").toList))
    }
    builder.execute()
  }


  def findOne (key : BSEEndOfDayStockPriceKey) : Option[BSEEndOfDayStockPrice] = {
    val doc = collection.findOne(MongoDBObject("_id.scripCode" -> key.scripCode,
                                               "_id.scripId" -> key.scripId,
                                               "_id.scripName" -> key.scripName,
                                               "_id.date" -> key.date)).getOrElse(return None)
    Some(BSEEndOfDayStockPriceMap.fromBsom(doc))
  }
}


//Test code
object BSEEndOfDayStockPriceDAOManagerTest {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val key = BSEEndOfDayStockPriceKey(500015, "APPLEFIN", "APPLE FINANCE LTD.", "28-January-2015")
    val bseEndOfDayStockPrice = BSEEndOfDayStockPrice(key, 2.7,2.62,2.27,2.50,2.497786522380718150,4066,31,10156.00,3960,97.39,0.35,0.23)

    val context = new MongoContext
    context.connect()

    val dao = new BSEEndOfDayStockPriceDAO(context.test_coll)
    dao.insert(bseEndOfDayStockPrice)
    println(dao.findOne(key))

    val bseEndOfDayStockPriceUpdated = BSEEndOfDayStockPrice(key, 3.7,3.62,3.27,3.50,3.497786522380718150,4066,31,10156.00,3960,97.39,0.35,0.23)
    dao.bulkUpdate(List(bseEndOfDayStockPriceUpdated))
    println(dao.findOne(key))

  }
}