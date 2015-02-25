package com.moneypenny.model

/**
 * Created by vives on 2/23/15.
 */
import java.util.Date

import com.moneypenny.util.CaseClassToMapImplicits
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

case class BSEClientCategorywiseTurnoverStatsKey (currentDate : Date, key : BSEClientCategorywiseTurnoverKey)

case class BSEClientCategorywiseTurnoverStats (_id : BSEClientCategorywiseTurnoverStatsKey, lastRun : Date, status : String)

object BSEClientCategorywiseTurnoverStatsMap {
  def toBson(bseClientCategorywiseTurnoverStats : BSEClientCategorywiseTurnoverStats) = {
    grater[BSEClientCategorywiseTurnoverStats].asDBObject(bseClientCategorywiseTurnoverStats)
  }

  def fromBsom(o: DBObject) : BSEClientCategorywiseTurnoverStats = {
    grater[BSEClientCategorywiseTurnoverStats].asObject(o)
  }
}

class BSEClientCategorywiseTurnoverStatsDAO (collection : MongoCollection) {
  def insert(bseClientCategorywiseTurnoverStats : BSEClientCategorywiseTurnoverStats) = {
    val doc = BSEClientCategorywiseTurnoverStatsMap.toBson(bseClientCategorywiseTurnoverStats)
    collection.insert(doc)
  }

  def bulkInsert(bseClientCategorywiseTurnoverStatsList : List[BSEClientCategorywiseTurnoverStats]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseClientCategorywiseTurnoverStatsList map {
      case bseClientCategorywiseTurnoverStats => builder.insert(BSEClientCategorywiseTurnoverStatsMap.toBson(bseClientCategorywiseTurnoverStats))
    }
    builder.execute()
  }

  def update(bseClientCategorywiseTurnoverStats : BSEClientCategorywiseTurnoverStats) = {
    val query = MongoDBObject("_id.currentDate" -> bseClientCategorywiseTurnoverStats._id.currentDate,
                              "_id.key.tradeDate" -> bseClientCategorywiseTurnoverStats._id.key.tradeDate)
    val doc = BSEClientCategorywiseTurnoverStatsMap.toBson(bseClientCategorywiseTurnoverStats)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate(bseClientCategorywiseTurnoverStatsList : List[BSEClientCategorywiseTurnoverStats]) = {
    import CaseClassToMapImplicits._
    val builder = collection.initializeOrderedBulkOperation
    bseClientCategorywiseTurnoverStatsList map {
      case bseClientCategorywiseTurnoverStats => builder.find(
        MongoDBObject("_id.currentDate" -> bseClientCategorywiseTurnoverStats._id.currentDate,
        "_id.key.tradeDate" -> bseClientCategorywiseTurnoverStats._id.key.tradeDate)).upsert().replaceOne(
          MongoDBObject(bseClientCategorywiseTurnoverStats.toStringWithFields.filterKeys(_ != "_id").toList))
    }
    builder.execute()
  }


  def findOne (key : BSEClientCategorywiseTurnoverStatsKey) : Option[BSEClientCategorywiseTurnoverStats] = {
    val doc = collection.findOne(MongoDBObject("_id.currentDate" -> key.currentDate)).getOrElse(return None)
    Some(BSEClientCategorywiseTurnoverStatsMap.fromBsom(doc))
  }

  def findLatest () : Option[BSEClientCategorywiseTurnoverStats] = {
    val docs = collection.find().sort(MongoDBObject("lastRun" -> -1)).limit(1).toList
    docs match {
      case doc :: Nil => Some(BSEClientCategorywiseTurnoverStatsMap.fromBsom(doc))
      case _ => None
    }
  }
}
