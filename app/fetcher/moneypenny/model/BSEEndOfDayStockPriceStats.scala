package com.moneypenny.model

/**
 * Created by vives on 2/25/15.
 */
import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

case class BSEEndOfDayStockPriceStatsKey (currentDate : Date, key : BSEEndOfDayStockPriceKey)

case class BSEEndOfDayStockPriceStats (_id : BSEEndOfDayStockPriceStatsKey, lastRun : Date, status : String)

object BSEEndOfDayStockPriceStatsMap {
  def toBson(bseEndOfDayStockPriceStats : BSEEndOfDayStockPriceStats) = {
    grater[BSEEndOfDayStockPriceStats].asDBObject(bseEndOfDayStockPriceStats)
  }

  def fromBsom(o: DBObject) : BSEEndOfDayStockPriceStats = {
    grater[BSEEndOfDayStockPriceStats].asObject(o)
  }
}

class BSEEndOfDayStockPriceStatsDAO (collection : MongoCollection) {
  def insert(bseEndOfDayStockPriceStats : BSEEndOfDayStockPriceStats) = {
    val doc = BSEEndOfDayStockPriceStatsMap.toBson(bseEndOfDayStockPriceStats)
    collection.insert(doc)
  }

  def bulkInsert(bseEndOfDayStockPriceStatsList : List[BSEEndOfDayStockPriceStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseEndOfDayStockPriceStatsList map {
      case bseEndOfDayStockPriceStats => builder.insert(BSEEndOfDayStockPriceStatsMap.toBson(bseEndOfDayStockPriceStats))
    }
    builder.execute()
  }

  def update(bseEndOfDayStockPriceStats : BSEEndOfDayStockPriceStats) = {
    val query = MongoDBObject("_id.currentDate" -> bseEndOfDayStockPriceStats._id.currentDate,
      "_id.key.scripCode" -> bseEndOfDayStockPriceStats._id.key.scripCode,
      "_id.key.scripId" -> bseEndOfDayStockPriceStats._id.key.scripId,
      "_id.key.scripName" -> bseEndOfDayStockPriceStats._id.key.scripName,
      "_id.key.date" -> bseEndOfDayStockPriceStats._id.key.tradeDate)
    val doc = BSEEndOfDayStockPriceStatsMap.toBson(bseEndOfDayStockPriceStats)
    collection.update(query, doc, upsert=true)
  }


  def bulkUpdate(bseEndOfDayStockPriceStatsList : List[BSEEndOfDayStockPriceStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseEndOfDayStockPriceStatsList map {
      case bseEndOfDayStockPriceStats => builder.find(MongoDBObject("_id.currentDate" -> bseEndOfDayStockPriceStats._id.currentDate,
            "_id.key.scripCode" -> bseEndOfDayStockPriceStats._id.key.scripCode,
            "_id.key.scripId" -> bseEndOfDayStockPriceStats._id.key.scripId,
            "_id.key.scripName" -> bseEndOfDayStockPriceStats._id.key.scripName,
            "_id.key.date" -> bseEndOfDayStockPriceStats._id.key.tradeDate)).upsert().update(
              new BasicDBObject("$set",BSEEndOfDayStockPriceStatsMap.toBson(bseEndOfDayStockPriceStats)))
      }
    builder.execute()
  }


  def findOne (key : BSEEndOfDayStockPriceStatsKey) : Option[BSEEndOfDayStockPriceStats] = {
    val doc = collection.findOne(MongoDBObject("_id.currentDate" -> key.currentDate)).getOrElse(return None)
    Some(BSEEndOfDayStockPriceStatsMap.fromBsom(doc))
  }

  def findLatest () : Option[BSEEndOfDayStockPriceStats] = {
    val docs = collection.find().sort(MongoDBObject("lastRun" -> -1)).limit(1).toList
    docs match {
      case doc :: Nil => Some(BSEEndOfDayStockPriceStatsMap.fromBsom(doc))
      case _ => None
    }
  }

  def findAll = {
    val doc = collection.find()
    for (element <- doc) yield BSEEndOfDayStockPriceStatsMap.fromBsom(element)
  }
}

