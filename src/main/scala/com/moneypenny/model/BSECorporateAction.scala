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
 * Created by vives on 2/13/15.
 */
case class BSECorporateActionKey (scripCode : Long, scripId : String, scripName : String, date : Date)

case class BSECorporateAction (_id : BSECorporateActionKey,
                               exDate : Option[Date],
                               purpose : Option[String],
                               recordDate : Option[Date],
                               bcStartDate : Option[Date],
                               bcEndDate : Option[Date],
                               ndStartDate : Option[Date],
                               ndEndDate : Option[Date],
                               actualPaymentDate : Option[Date])


object BSECorporateActionMap {
  def toBson(bseCorporateAction : BSECorporateAction) = {
    grater[BSECorporateAction].asDBObject(bseCorporateAction)
  }

  def fromBsom(o: DBObject) : BSECorporateAction = {
    grater[BSECorporateAction].asObject(o)
  }
}

class BSECorporateActionDAO (collection : MongoCollection) {
  def insert(bseCorporateAction : BSECorporateAction) = {
    val doc = BSECorporateActionMap.toBson(bseCorporateAction)
    collection.insert(doc)
  }

  def update(bseCorporateAction : BSECorporateAction) = {
    val query = MongoDBObject("_id.scripCode" -> bseCorporateAction._id.scripCode,
      "_id.scripId" -> bseCorporateAction._id.scripId,
      "_id.scripName" -> bseCorporateAction._id.scripName,
      "_id.date" -> bseCorporateAction._id.date)
    val doc = BSECorporateActionMap.toBson(bseCorporateAction)
    collection.update(query, doc, upsert=true)
  }

  def findOne (key : BSECorporateActionKey) : Option[BSECorporateAction] = {
    val doc = collection.findOne(MongoDBObject("_id.scripCode" -> key.scripCode,
      "_id.scripId" -> key.scripId,
      "_id.scripName" -> key.scripName,
      "_id.date" -> key.date)).getOrElse(return None)
    Some(BSECorporateActionMap.fromBsom(doc))
  }
}

object BSECorporateActionDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()
    RegisterJodaLocalDateTimeConversionHelpers

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    val localDate = dtf.parseLocalDateTime("28-January-2015 15:45:00")

    val key = BSECorporateActionKey(500043,"BATAINDIA","BATA INDIA LTD.", localDate.toDate)
    val bseCorporateAction = BSECorporateAction(key,Some(localDate.toDate),Some("Dividend - Rs.1.50"),None,
      Some(localDate.toDate),Some(localDate.toDate), Some(localDate.toDate),Some(localDate.toDate),None)

    val context = new MongoContext
    context.connect()

    val dao = new BSECorporateActionDAO(context.test_coll)

    dao.insert(bseCorporateAction)

    println(dao.findOne(key))
    val bseCorporateActionUpdated = BSECorporateAction(key, Some(localDate.toDate),None,None,None,None,None,None,null)
    dao.update(bseCorporateActionUpdated)
    println(dao.findOne(key))
  }
}