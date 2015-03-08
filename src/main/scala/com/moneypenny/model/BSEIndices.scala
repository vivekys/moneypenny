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
//http://www.bseindia.com/indices/indexarchivedata.aspx
case class BSEIndicesKey (indexId : String, indexName : String, tradeDate : Date)

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
    val builder = collection.initializeUnorderedBulkOperation
    bseIndicesList map {
      case bseIndices => builder.insert(BSEIndicesMap.toBson(bseIndices))
    }
    builder.execute()
  }

  def update(bseIndices : BSEIndices) = {
    val query = MongoDBObject("_id.indexId" -> bseIndices._id.indexId,
      "_id.indexName" -> bseIndices._id.indexName,
      "_id.tradeDate" -> bseIndices._id.tradeDate)
    val doc = BSEIndicesMap.toBson(bseIndices)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (bseIndicesList : List[BSEIndices]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseIndicesList map {
      case bseIndices => builder.find(MongoDBObject("_id.indexId" -> bseIndices._id.indexId,
        "_id.indexName" -> bseIndices._id.indexName,
        "_id.tradeDate" -> bseIndices._id.tradeDate)).upsert().update(
          new BasicDBObject("$set",BSEIndicesMap.toBson(bseIndices)))
    }
    builder.execute()
  }

  def findOne (key : BSEIndicesKey) : Option[BSEIndices] = {
    val doc = collection.findOne(MongoDBObject("_id.indexId" -> key.indexId,
      "_id.indexName" -> key.indexName,
      "_id.tradeDate" -> key.tradeDate)).getOrElse(return None)
    Some(BSEIndicesMap.fromBsom(doc))
  }

  def count = {
    collection.count()
  }

  def findAll = {
    val doc = collection.find()
    for (element <- doc) yield BSEIndicesMap.fromBsom(element)
  }

  def findLatest = {
    val aggregationOptions = AggregationOptions(AggregationOptions.CURSOR)
    val results = collection.aggregate(
      List(
        MongoDBObject(
          "$group" ->
            MongoDBObject("_id" -> MongoDBObject("indexId" -> "$_id.indexId",
                                                 "indexName" -> "$_id.indexName"),
                          "tradeDate" -> MongoDBObject("$max" -> "$_id.tradeDate"))
        )
      ), aggregationOptions
    )
    results map {
      case res =>
        val id = res.as[Map[String, String]]("_id")
        BSEIndicesKey(indexId = id.get("indexId").get,
          indexName = id.get("indexName").get, tradeDate = res.as[Date]("tradeDate")
      )
    }
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
    val key = BSEIndicesKey("BSEOIL","S&P BSE OIL & GAS", localDate.toDate)
    val bseIndices = BSEIndices(key, 9853.46,9936.45,9851.97,9904.86)

    val context = new MongoContext
    context.connect()

    val dao = new BSEIndicesDAO(context.test_coll)

//    dao.insert(bseIndices)

//    println(dao.findOne(key))
//    val bseIndicesUpdated = BSEIndices(key, 0,0,0,0)
//    val res = dao.bulkUpdate(List(bseIndicesUpdated))
//    println(res)
//    println(dao.findOne(key))
    val result = dao.findLatest
    for (res <- result) println(res)
  }
}