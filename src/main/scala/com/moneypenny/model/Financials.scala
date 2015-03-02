package com.moneypenny.model

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vives on 2/16/15.
 */
case class FinancialsKey (name : String, financial : String, financialType : String, fyDate : Date)

case class Financials (_id : FinancialsKey, data : Map[String, String])

object FinancialsMap {
  def toBson(financials : Financials) = {
    grater[Financials].asDBObject(financials)
  }

  def fromBsom(o: DBObject) : Financials = {
    grater[Financials].asObject(o)
  }
}

class FinancialsDAO (collection : MongoCollection) {
  def insert(financials : Financials) = {
    val doc = FinancialsMap.toBson(financials)
    collection.insert(doc)
  }

  def update(financials : Financials) = {
    val query = MongoDBObject("_id.name" -> financials._id.name,
      "_id.financial" -> financials._id.financial,
      "_id.financialType" -> financials._id.financialType,
      "_id.fyDate" -> financials._id.fyDate)
    val doc = FinancialsMap.toBson(financials)
    collection.update(query, doc, upsert=true)
  }

  def findOne (key : FinancialsKey) : Option[Financials] = {
    val doc = collection.findOne(MongoDBObject("_id.name" -> key.name,
      "_id.financial" -> key.financial,
      "_id.financialType" -> key.financialType,
      "_id.fyDate" -> key.fyDate)).getOrElse(return None)
    Some(FinancialsMap.fromBsom(doc))
  }
}


object FinancialsDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()
    RegisterJodaLocalDateTimeConversionHelpers

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

  }
}

