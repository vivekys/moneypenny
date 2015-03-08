package com.moneypenny.model

import com.moneypenny.db.MongoContext
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vives on 2/16/15.
 */
case class MoneycontrolListOfScripsKey (companyName : String, url : String)

case class MoneycontrolListOfScrips (_id : MoneycontrolListOfScripsKey, bseScripCode : Option[Long],
                                     nseScripCode : Option[String], sector : Option[String])

object MoneycontrolListOfScripsMap {
  def toBson(moneycontrolListOfScrips : MoneycontrolListOfScrips) = {
    grater[MoneycontrolListOfScrips].asDBObject(moneycontrolListOfScrips)
  }

  def fromBsom(o: DBObject) : MoneycontrolListOfScrips = {
    grater[MoneycontrolListOfScrips].asObject(o)
  }
}

class MoneycontrolListOfScripsDAO (collection : MongoCollection) {
  def insert(moneycontrolListOfScrips : MoneycontrolListOfScrips) = {
    val doc = MoneycontrolListOfScripsMap.toBson(moneycontrolListOfScrips)
    collection.insert(doc)
  }

  def bulkInsert (moneycontrolListOfScrips : List[MoneycontrolListOfScrips]) = {
    val builder = collection.initializeUnorderedBulkOperation
    moneycontrolListOfScrips map {
      case moneycontrolListOfScrip => builder.insert(MoneycontrolListOfScripsMap.toBson(moneycontrolListOfScrip))
    }
    builder.execute()
  }

  def update(moneycontrolListOfScrips : MoneycontrolListOfScrips) = {
    val query = MongoDBObject("_id.companyName" -> moneycontrolListOfScrips._id.companyName,
      "_id.url" -> moneycontrolListOfScrips._id.url)
    val doc = MoneycontrolListOfScripsMap.toBson(moneycontrolListOfScrips)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (moneycontrolListOfScrips : List[MoneycontrolListOfScrips]) = {
    val builder = collection.initializeUnorderedBulkOperation
    moneycontrolListOfScrips map {
      case moneycontrolListOfScrip => builder.find(MongoDBObject("_id.companyName" -> moneycontrolListOfScrip._id.companyName,
        "_id.url" -> moneycontrolListOfScrip._id.url)).upsert().update(
          new BasicDBObject("$set",MoneycontrolListOfScripsMap.toBson(moneycontrolListOfScrip)))
    }
    builder.execute()
  }


  def findOne (key : MoneycontrolListOfScripsKey) : Option[MoneycontrolListOfScrips] = {
    val doc = collection.findOne(MongoDBObject("_id.companyName" -> key.companyName,
      "_id.url" -> key.url)).getOrElse(return None)
    Some(MoneycontrolListOfScripsMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    for (element <- doc) yield MoneycontrolListOfScripsMap.fromBsom(element)
  }
}


object MoneycontrolListOfScripsDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val key = MoneycontrolListOfScripsKey("ABB India Limited", "testURL")
    val moneycontrolListOfScrips = MoneycontrolListOfScrips(key, Some(500002), Some("ABB"),Some("Heavy Electrical Equipment"))

    val context = new MongoContext
    context.connect()

    val dao = new MoneycontrolListOfScripsDAO(context.moneycontrolListOfScripsCollection)

    val res = dao.bulkUpdate(List(moneycontrolListOfScrips))
    println(res)
    println(dao.findOne(key))
  }
}


