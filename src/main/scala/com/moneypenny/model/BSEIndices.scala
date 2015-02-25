package com.moneypenny.model

import java.util.Date

import com.moneypenny.db.MongoContext
import com.moneypenny.util.CaseClassToMapImplicits
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
//http://www.bseindia.com/indices/indexarchivedata.aspx
case class BSEIndicesKey (index : String, date : Date)

case class BSEIndices(_id : BSEIndicesKey, open : Double, high : Double, low : Double, close : Double)

object BSEIndicesMap {
  def toBson(bseIndices : BSEIndices) = {
    grater[BSEIndices].asDBObject(bseIndices)
  }

  def fromBsom(o: DBObject) : BSEIndices = {
    grater[BSEIndices].asObject(o)
  }
}

class BSEIndicesDAO (collection : MongoCollection) {
  def insert(bseIndices : BSEIndices) = {
    val doc = BSEIndicesMap.toBson(bseIndices)
    collection.insert(doc)
  }

  def bulkInsert (bseIndicesList : List[BSEIndices]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseIndicesList map {
      case bseIndices => builder.insert(BSEIndicesMap.toBson(bseIndices))
    }
    builder.execute()
  }

  def update(bseIndices : BSEIndices) = {
    val query = MongoDBObject("_id.index" -> bseIndices._id.index,
      "_id.date" -> bseIndices._id.date)
    val doc = BSEIndicesMap.toBson(bseIndices)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (bseIndicesList : List[BSEIndices]) = {
    import CaseClassToMapImplicits._
    val builder = collection.initializeOrderedBulkOperation
    bseIndicesList map {
      case bseIndices => builder.find(MongoDBObject("_id.index" -> bseIndices._id.index,
        "_id.date" -> bseIndices._id.date)).upsert().replaceOne(
          MongoDBObject(bseIndices.toStringWithFields.filterKeys(_ != "_id").toList))
    }
    builder.execute()
  }

  def findOne (key : BSEIndicesKey) : Option[BSEIndices] = {
    val doc = collection.findOne(MongoDBObject("_id.index" -> key.index,
      "_id.date" -> key.date)).getOrElse(return None)
    Some(BSEIndicesMap.fromBsom(doc))
  }

  def count = {
    collection.count()
  }
}

object BSEIndicesDAOManagerTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()
    RegisterJodaLocalDateTimeConversionHelpers

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")

    val localDate = dtf.parseLocalDateTime("1-January-2015 15:45:00")
    val key = BSEIndicesKey("S&P BSE OIL & GAS", localDate.toDate)
    val bseIndices = BSEIndices(key, 9853.46,9936.45,9851.97,9904.86)

    val context = new MongoContext
    context.connect()

    val dao = new BSEIndicesDAO(context.test_coll)

    dao.insert(bseIndices)

    println(dao.findOne(key))
    val bseIndicesUpdated = BSEIndices(key, 0,0,0,0)
    dao.bulkUpdate(List(bseIndicesUpdated))
    println(dao.findOne(key))

  }
}