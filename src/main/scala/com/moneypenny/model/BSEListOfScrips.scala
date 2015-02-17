package com.moneypenny.model

import com.moneypenny.db.MongoContext
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vives on 11/30/14.
 */

//http://www.bseindia.com/corporates/List_Scrips.aspx?expandable=1
case class BSEListOfScripsKey (scripCode : Long)

case class BSEListOfScrips(_id : BSEListOfScripsKey, scripId : Option[String], scripName : Option[String],
                           status : Option[String], group: Option[String], faceValue : Option[Double],
                           ISINNo : Option[String], industry: Option[String], instrument: Option[String])

object BSEListOfScripsMap {
  def toBson(bseListOfScrips : BSEListOfScrips) = {
    grater[BSEListOfScrips].asDBObject(bseListOfScrips)
  }

  def fromBsom(o: DBObject) : BSEListOfScrips = {
    grater[BSEListOfScrips].asObject(o)
  }
}

class BSEListOfScripsDAO (collection : MongoCollection) {
  def insert(bseListOfScrips : BSEListOfScrips) = {
    val doc = BSEListOfScripsMap.toBson(bseListOfScrips)
    collection.insert(doc)
  }

  def update(bseListOfScrips : BSEListOfScrips) = {
    val query = MongoDBObject("_id.scripCode" -> bseListOfScrips._id.scripCode)
    val doc = BSEListOfScripsMap.toBson(bseListOfScrips)
    collection.update(query, doc, upsert=true)
  }

  def findOne (key : BSEListOfScripsKey) : Option[BSEListOfScrips] = {
    val doc = collection.findOne(MongoDBObject("_id.scripCode" -> key.scripCode)).getOrElse(return None)
    Some(BSEListOfScripsMap.fromBsom(doc))
  }
}


object BSEListOfScripsDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()
    RegisterJodaLocalDateTimeConversionHelpers

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val key = BSEListOfScripsKey(500002)
    val bseListOfScrips = BSEListOfScrips(key,Some("ABB"),Some("ABB India Limited"),Some("Active"),Some("A") ,
                      Some(2.00),Some("INE117A01022"), Some("Heavy Electrical Equipment"),Some("Equity"))

    val context = new MongoContext
    context.connect()

    val dao = new BSEListOfScripsDAO(context.test_coll)

    dao.insert(bseListOfScrips)

    println(dao.findOne(key))
    val bseListOfScripsUpdated = BSEListOfScrips(key,Some("ABB"),Some("ABB India Limited"),Some("Active"),Some("A") ,
      Some(2.00),Some("INE117A01022"), None,Some("Equity"))

    dao.update(bseListOfScripsUpdated)
    println(dao.findOne(key))
  }
}


