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
case class BSECorporateActionKey (scripCode : Long, scripId : String, scripName : String, exDate : Option[Date],
                                  purpose : Option[String],
                                  recordDate : Option[Date],
                                  bcStartDate : Option[Date],
                                  bcEndDate : Option[Date],
                                  ndStartDate : Option[Date],
                                  ndEndDate : Option[Date],
                                  actualPaymentDate : Option[Date])

case class BSECorporateAction (_id : BSECorporateActionKey)


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

  def bulkInsert (bseCorporateActionList : List[BSECorporateAction]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseCorporateActionList map {
      case bseCorporateAction => builder.insert(BSECorporateActionMap.toBson(bseCorporateAction))
    }
    builder.execute()
  }

  def update(bseCorporateAction : BSECorporateAction) = {
    val query = MongoDBObject("_id.scripCode" -> bseCorporateAction._id.scripCode,
      "_id.scripId" -> bseCorporateAction._id.scripId,
      "_id.scripName" -> bseCorporateAction._id.scripName,
      "_id.exDate" -> bseCorporateAction._id.exDate,
      "_id.purpose" -> bseCorporateAction._id.purpose,
      "_id.recordDate" -> bseCorporateAction._id.recordDate,
      "_id.bcStartDate" -> bseCorporateAction._id.bcStartDate,
      "_id.bcEndDate" -> bseCorporateAction._id.bcEndDate,
      "_id.ndStartDate" -> bseCorporateAction._id.ndStartDate,
      "_id.ndEndDate" -> bseCorporateAction._id.ndEndDate,
      "_id.actualPaymentDate" -> bseCorporateAction._id.actualPaymentDate)
    val doc = BSECorporateActionMap.toBson(bseCorporateAction)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (bseCorporateActionList : List[BSECorporateAction]) = {
    val builder = collection.initializeUnorderedBulkOperation
    bseCorporateActionList map {
      case bseCorporateAction => builder.find(MongoDBObject("_id.scripCode" -> bseCorporateAction._id.scripCode,
        "_id.scripId" -> bseCorporateAction._id.scripId,
        "_id.scripName" -> bseCorporateAction._id.scripName,
        "_id.exDate" -> bseCorporateAction._id.exDate,
        "_id.purpose" -> bseCorporateAction._id.purpose,
        "_id.recordDate" -> bseCorporateAction._id.recordDate,
        "_id.bcStartDate" -> bseCorporateAction._id.bcStartDate,
        "_id.bcEndDate" -> bseCorporateAction._id.bcEndDate,
        "_id.ndStartDate" -> bseCorporateAction._id.ndStartDate,
        "_id.ndEndDate" -> bseCorporateAction._id.ndEndDate,
        "_id.actualPaymentDate" -> bseCorporateAction._id.actualPaymentDate)).upsert().update(
          new BasicDBObject("$set",BSECorporateActionMap.toBson(bseCorporateAction)))
    }
    builder.execute()
  }

  def findOne (key : BSECorporateActionKey) : Option[BSECorporateAction] = {
    val doc = collection.findOne(MongoDBObject("_id.scripCode" -> key.scripCode,
      "_id.scripId" -> key.scripId,
      "_id.scripName" -> key.scripName,
      "_id.exDate" -> key.exDate,
      "_id.purpose" -> key.purpose,
      "_id.recordDate" -> key.recordDate,
      "_id.bcStartDate" -> key.bcStartDate,
      "_id.bcEndDate" -> key.bcEndDate,
      "_id.ndStartDate" -> key.ndStartDate,
      "_id.ndEndDate" -> key.ndEndDate,
      "_id.actualPaymentDate" -> key.actualPaymentDate)).getOrElse(return None)
    Some(BSECorporateActionMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    for (element <- doc) yield BSECorporateActionMap.fromBsom(element)
  }
}

object BSECorporateActionDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    val localDate = dtf.parseLocalDateTime("28-January-2015 15:45:00")

    val key = BSECorporateActionKey(500043,"BATAINDIA","BATA INDIA LTD.", Some(localDate.toDate),Some("Dividend - Rs.1.50"),None,
      Some(localDate.toDate),Some(localDate.toDate), Some(localDate.toDate),Some(localDate.toDate),None)
    val bseCorporateAction = BSECorporateAction(key)

    val context = new MongoContext
    context.connect()

    val dao = new BSECorporateActionDAO(context.test_coll)

    dao.insert(bseCorporateAction)

    println(dao.findOne(key))
  }
}

