package com.moneypenny.model

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vives on 2/25/15.
 */
case class BSETradingHighlightsStatsKey (currentDate : Date, key : BSETradingHighlightsKey)

case class BSETradingHighlightsStats (_id : BSETradingHighlightsStatsKey, lastRun : Date, status : String)

object BSETradingHighlightsStatsMap {
  def toBson(bseTradingHighlightsStats : BSETradingHighlightsStats) = {
    grater[BSETradingHighlightsStats].asDBObject(bseTradingHighlightsStats)
  }

  def fromBsom(o: DBObject) : BSETradingHighlightsStats = {
    grater[BSETradingHighlightsStats].asObject(o)
  }
}

class BSETradingHighlightsStatsDAO (collection : MongoCollection) {
  def insert(bseTradingHighlightsStats : BSETradingHighlightsStats) = {
    val doc = BSETradingHighlightsStatsMap.toBson(bseTradingHighlightsStats)
    collection.insert(doc)
  }

  def bulkInsert(bseTradingHighlightsStatsList : List[BSETradingHighlightsStats]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseTradingHighlightsStatsList map {
      case bseTradingHighlightsStats => builder.insert(BSETradingHighlightsStatsMap.toBson(bseTradingHighlightsStats))
    }
    builder.execute()
  }

  def update(bseTradingHighlightsStats : BSETradingHighlightsStats) = {
    val query = MongoDBObject("_id.currentDate" -> bseTradingHighlightsStats._id.currentDate,
      "_id.key.date" -> bseTradingHighlightsStats._id.key.date)
    val doc = BSETradingHighlightsStatsMap.toBson(bseTradingHighlightsStats)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate(bseTradingHighlightsStatsList : List[BSETradingHighlightsStats]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseTradingHighlightsStatsList map {
      case bseTradingHighlightsStats => builder.find(MongoDBObject("_id.currentDate" -> bseTradingHighlightsStats._id.currentDate,
        "_id.key.date" -> bseTradingHighlightsStats._id.key.date)).upsert().update(
          new BasicDBObject("$set",BSETradingHighlightsStatsMap.toBson(bseTradingHighlightsStats)))
    }
    builder.execute()
  }

  def findOne (key : BSETradingHighlightsStatsKey) : Option[BSETradingHighlightsStats] = {
    val doc = collection.findOne(MongoDBObject("_id.currentDate" -> key.currentDate,
      "_id.key.date" -> key.key.date)).getOrElse(return None)
    Some(BSETradingHighlightsStatsMap.fromBsom(doc))
  }

  def findLatest () : Option[BSETradingHighlightsStats] = {
    val docs = collection.find().sort(MongoDBObject("lastRun" -> -1)).limit(1).toList
    docs match {
      case doc :: Nil => Some(BSETradingHighlightsStatsMap.fromBsom(doc))
      case _ => None
    }
  }
}

