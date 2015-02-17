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
case class MoneycontrolListOfScripsKey (scripCode : Long)

case class MoneycontrolListOfScrips (_id : MoneycontrolListOfScripsKey, bseScripCode : Long, nseScripCode : Option[String],
                                     name : Option[String], sector : Option[String])

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

  def update(moneycontrolListOfScrips : MoneycontrolListOfScrips) = {
    val query = MongoDBObject("_id.scripCode" -> moneycontrolListOfScrips._id.scripCode)
    val doc = MoneycontrolListOfScripsMap.toBson(moneycontrolListOfScrips)
    collection.update(query, doc, upsert=true)
  }

  def findOne (key : MoneycontrolListOfScripsKey) : Option[MoneycontrolListOfScrips] = {
    val doc = collection.findOne(MongoDBObject("_id.scripCode" -> key.scripCode)).getOrElse(return None)
    Some(MoneycontrolListOfScripsMap.fromBsom(doc))
  }
}


object MoneycontrolListOfScripsDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()
    RegisterJodaLocalDateTimeConversionHelpers

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val key = MoneycontrolListOfScripsKey(500002)
    val moneycontrolListOfScrips = MoneycontrolListOfScrips(key, 500002, Some("ABB"),Some("ABB India Limited"),
      Some("Heavy Electrical Equipment"))

    val context = new MongoContext
    context.connect()

    val dao = new MoneycontrolListOfScripsDAO(context.test_coll)

    dao.insert(moneycontrolListOfScrips)

    println(dao.findOne(key))
    val bseListOfScripsUpdated = MoneycontrolListOfScrips(key, 500002, Some("ABB-Updated"),Some("ABB India Limited"),
      Some("Heavy Electrical Equipment"))

    dao.update(bseListOfScripsUpdated)
    println(dao.findOne(key))
  }
}


