package com.moneypenny.model

/**
 * Created by vives on 2/25/15.
 */
import java.util.Date

import com.moneypenny.util.CaseClassToMapImplicits
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

case class BSECorporateActionStatsKey (currentDate : Date, key : BSECorporateActionKey)

case class BSECorporateActionStats (_id : BSECorporateActionStatsKey, lastRun : Date, status : String)

object BSECorporateActionStatsMap {
  def toBson(bseCorporateActionStats : BSECorporateActionStats) = {
    grater[BSECorporateActionStats].asDBObject(bseCorporateActionStats)
  }

  def fromBsom(o: DBObject) : BSECorporateActionStats = {
    grater[BSECorporateActionStats].asObject(o)
  }
}

class BSECorporateActionStatsDAO (collection : MongoCollection) {
  def insert(bseCorporateActionStats : BSECorporateActionStats) = {
    val doc = BSECorporateActionStatsMap.toBson(bseCorporateActionStats)
    collection.insert(doc)
  }

  def bulkInsert(bseCorporateActionStatsList : List[BSECorporateActionStats]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseCorporateActionStatsList map {
      case bseCorporateActionStats => builder.insert(BSECorporateActionStatsMap.toBson(bseCorporateActionStats))
    }
    builder.execute()
  }

  def update(bseCorporateActionStats : BSECorporateActionStats) = {
    val query = MongoDBObject("_id.currentDate" -> bseCorporateActionStats._id.currentDate,
      "_id.key.scripCode" -> bseCorporateActionStats._id.key.scripCode,
      "_id.key.scripId" -> bseCorporateActionStats._id.key.scripId,
      "_id.key.scripName" -> bseCorporateActionStats._id.key.scripName,
      "_id.key.date" -> bseCorporateActionStats._id.key.date)
    val doc = BSECorporateActionStatsMap.toBson(bseCorporateActionStats)
    collection.update(query, doc, upsert=true)
  }


  def bulkUpdate(bseCorporateActionStatsList : List[BSECorporateActionStats]) = {
    import CaseClassToMapImplicits._
    val builder = collection.initializeOrderedBulkOperation
    bseCorporateActionStatsList map {
      case bseCorporateActionStats => builder.find(MongoDBObject("_id.currentDate" -> bseCorporateActionStats._id.currentDate,
        "_id.key.scripCode" -> bseCorporateActionStats._id.key.scripCode,
        "_id.key.scripId" -> bseCorporateActionStats._id.key.scripId,
        "_id.key.scripName" -> bseCorporateActionStats._id.key.scripName,
        "_id.key.date" -> bseCorporateActionStats._id.key.date)).upsert().replaceOne(
          MongoDBObject(bseCorporateActionStats.toStringWithFields.filterKeys(_ != "_id").toList))
    }
    builder.execute()
  }


  def findOne (key : BSECorporateActionStatsKey) : Option[BSECorporateActionStats] = {
    val doc = collection.findOne(MongoDBObject("_id.currentDate" -> key.currentDate)).getOrElse(return None)
    Some(BSECorporateActionStatsMap.fromBsom(doc))
  }

  def findLatest () : Option[BSECorporateActionStats] = {
    val docs = collection.find().sort(MongoDBObject("lastRun" -> -1)).limit(1).toList
    docs match {
      case doc :: Nil => Some(BSECorporateActionStatsMap.fromBsom(doc))
      case _ => None
    }
  }
}
