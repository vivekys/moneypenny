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

case class BSEEquityMarketSummaryStatsKey (currentDate : Date, key : BSEEquityMarketSummaryKey)

case class BSEEquityMarketSummaryStats (_id : BSEEquityMarketSummaryStatsKey, lastRun : Date, status : String)

object BSEEquityMarketSummaryStatsMap {
  def toBson(bseEquityMarketSummaryStats : BSEEquityMarketSummaryStats) = {
    grater[BSEEquityMarketSummaryStats].asDBObject(bseEquityMarketSummaryStats)
  }

  def fromBsom(o: DBObject) : BSEEquityMarketSummaryStats = {
    grater[BSEEquityMarketSummaryStats].asObject(o)
  }
}

class BSEEquityMarketSummaryStatsDAO (collection : MongoCollection) {
  def insert(bseEquityMarketSummaryStats : BSEEquityMarketSummaryStats) = {
    val doc = BSEEquityMarketSummaryStatsMap.toBson(bseEquityMarketSummaryStats)
    collection.insert(doc)
  }

  def bulkInsert(bseEquityMarketSummaryStatsList : List[BSEEquityMarketSummaryStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseEquityMarketSummaryStatsList map {
      case bseEquityMarketSummaryStats => builder.insert(BSEEquityMarketSummaryStatsMap.toBson(bseEquityMarketSummaryStats))
    }
    builder.execute()
  }

  def update(bseEquityMarketSummaryStats : BSEEquityMarketSummaryStats) = {
    val query = MongoDBObject("_id.currentDate" -> bseEquityMarketSummaryStats._id.currentDate,
      "_id.key.date" -> bseEquityMarketSummaryStats._id.key.date)
    val doc = BSEEquityMarketSummaryStatsMap.toBson(bseEquityMarketSummaryStats)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate(bseEquityMarketSummaryStatsList : List[BSEEquityMarketSummaryStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseEquityMarketSummaryStatsList map {
      case bseEquityMarketSummaryStats => builder.find(
        MongoDBObject("_id.currentDate" -> bseEquityMarketSummaryStats._id.currentDate,
        "_id.key.date" -> bseEquityMarketSummaryStats._id.key.date)).upsert().update(
          new BasicDBObject("$set",BSEEquityMarketSummaryStatsMap.toBson(bseEquityMarketSummaryStats)))
    }
    builder.execute()
  }

  def findOne (key : BSEEquityMarketSummaryStatsKey) : Option[BSEEquityMarketSummaryStats] = {
    val doc = collection.findOne(MongoDBObject("_id.currentDate" -> key.currentDate)).getOrElse(return None)
    Some(BSEEquityMarketSummaryStatsMap.fromBsom(doc))
  }

  def findLatest () : Option[BSEEquityMarketSummaryStats] = {
    val docs = collection.find().sort(MongoDBObject("lastRun" -> -1)).limit(1).toList
    docs match {
      case doc :: Nil => Some(BSEEquityMarketSummaryStatsMap.fromBsom(doc))
      case _ => None
    }
  }

  def findAll = {
    val doc = collection.find()
    for (element <- doc) yield BSEEquityMarketSummaryStatsMap.fromBsom(element)
  }
}
