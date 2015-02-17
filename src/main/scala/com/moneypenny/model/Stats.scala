package com.moneypenny.model

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._

/**
 * Created by vives on 2/17/15.
 */
case class StatsKey (currentDate : Date)

case class Stats (_id : StatsKey, lastRun : Date, status : String)

object StatsMap {
  def toBson(bseIndicesStats : Stats) = {
    grater[Stats].asDBObject(bseIndicesStats)
  }

  def fromBsom(o: DBObject) : Stats = {
    grater[Stats].asObject(o)
  }
}

class StatsDAO (collection : MongoCollection) {
  def insert(bseIndicesStats : Stats) = {
    val doc = StatsMap.toBson(bseIndicesStats)
    collection.insert(doc)
  }

  def update(bseIndicesStats : Stats) = {
    val query = MongoDBObject("_id.currentDate" -> bseIndicesStats._id.currentDate)
    val doc = StatsMap.toBson(bseIndicesStats)
    collection.update(query, doc, upsert=true)
  }

  def findOne (key : StatsKey) : Option[Stats] = {
    val doc = collection.findOne(MongoDBObject("_id.currentDate" -> key.currentDate)).getOrElse(return None)
    Some(StatsMap.fromBsom(doc))
  }
}

