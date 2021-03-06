package com.moneypenny.model

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vives on 2/17/15.
 */
case class BSEIndicesStatsKey (currentDate : Date, key : BSEIndicesKey)

case class BSEIndicesStats (_id : BSEIndicesStatsKey, lastRun : Date, status : String)

object BSEIndicesStatsMap {
  def toBson(bseIndicesStats : BSEIndicesStats) = {
    grater[BSEIndicesStats].asDBObject(bseIndicesStats)
  }

  def fromBsom(o: DBObject) : BSEIndicesStats = {
    grater[BSEIndicesStats].asObject(o)
  }
}

class BSEIndicesStatsDAO (collection : MongoCollection) {
  def insert(bseIndicesStats : BSEIndicesStats) = {
    val doc = BSEIndicesStatsMap.toBson(bseIndicesStats)
    collection.insert(doc)
  }

  def bulkInsert(bseIndicesStatsList : List[BSEIndicesStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseIndicesStatsList map {
      case bseIndicesStats => builder.insert(BSEIndicesStatsMap.toBson(bseIndicesStats))
    }
    builder.execute()
  }

  def update(bseIndicesStats : BSEIndicesStats) = {
    val query = MongoDBObject("_id.currentDate" -> bseIndicesStats._id.currentDate,
      "_id.key.indexId" -> bseIndicesStats._id.key.indexId,
      "_id.key.indexName" -> bseIndicesStats._id.key.indexName,
      "_id.key.tradeDate" -> bseIndicesStats._id.key.tradeDate)
    val doc = BSEIndicesStatsMap.toBson(bseIndicesStats)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate(bseIndicesStatsList : List[BSEIndicesStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseIndicesStatsList map {
      case bseIndicesStats => builder.find(MongoDBObject("_id.currentDate" -> bseIndicesStats._id.currentDate,
        "_id.key.indexId" -> bseIndicesStats._id.key.indexId,
        "_id.key.indexName" -> bseIndicesStats._id.key.indexName,
        "_id.key.tradeDate" -> bseIndicesStats._id.key.tradeDate)).upsert().update(
          new BasicDBObject("$set",BSEIndicesStatsMap.toBson(bseIndicesStats)))
    }
    builder.execute()
  }

  def findOne (key : BSEIndicesStatsKey) : Option[BSEIndicesStats] = {
    val doc = collection.findOne(MongoDBObject("_id.currentDate" -> key.currentDate)).getOrElse(return None)
    Some(BSEIndicesStatsMap.fromBsom(doc))
  }

  def findLatest () : Option[BSEIndicesStats] = {
    val docs = collection.find().sort(MongoDBObject("lastRun" -> -1)).limit(1).toList
    docs match {
      case doc :: Nil => Some(BSEIndicesStatsMap.fromBsom(doc))
      case _ => None
    }
  }

  def findAll = {
    val doc = collection.find()
    for (element <- doc) yield BSEIndicesStatsMap.fromBsom(element)
  }
}

