package com.moneypenny.model

import java.util.Date

import com.moneypenny.db.MongoContext
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{BasicDBObject, DBObject}
import com.novus.salat._
import com.novus.salat.global._
import org.joda.time.format.DateTimeFormat

/**
 * Created by vives on 1/1/15.
 */
//http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&flag=1
case class BSEClientCategorywiseTurnoverKey (tradeDate : Date)

case class BSEClientCategorywiseTurnover (_id : BSEClientCategorywiseTurnoverKey,
                                          clientsBuy : Double, clientsSales : Double, clientsNet	: Double,
                                          nriBuy : Double, nriSales : Double, nriNet : Double,
                                          proprietaryBuy : Double, proprietarySales : Double, proprietaryNet : Double,
                                          IFIsBuy : Double,	IFIsSales : Double, IFIsNet : Double,
                                          banksBuy : Double, banksSales : Double,	banksNet : Double,
                                          insuranceBuy : Double, insuranceSales : Double,	insuranceNet : Double,
                                          DIIBuy : Double, DIISales : Double,	DIINet : Double)

object BSEClientCategorywiseTurnoverMap {
  def toBson(bseClientCategorywiseTurnover : BSEClientCategorywiseTurnover) = {
    grater[BSEClientCategorywiseTurnover].asDBObject(bseClientCategorywiseTurnover)
  }

  def fromBsom(o: DBObject) : BSEClientCategorywiseTurnover = {
    grater[BSEClientCategorywiseTurnover].asObject(o)
  }
}

class BSEClientCategorywiseTurnoverDAO (collection : MongoCollection) {
  def insert(bseClientCategorywiseTurnover : BSEClientCategorywiseTurnover) = {
    val doc = BSEClientCategorywiseTurnoverMap.toBson(bseClientCategorywiseTurnover)
    collection.insert(doc)
  }

  def bulkInsert (bseClientCategorywiseTurnoverList : List[BSEClientCategorywiseTurnover]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseClientCategorywiseTurnoverList map {
      case bseClientCategorywiseTurnover => builder.insert(BSEClientCategorywiseTurnoverMap.toBson(bseClientCategorywiseTurnover))
    }
    builder.execute()
  }


  def update(bseClientCategorywiseTurnover : BSEClientCategorywiseTurnover) = {
    val query = MongoDBObject("_id.tradeDate" -> bseClientCategorywiseTurnover._id.tradeDate)
    val doc = BSEClientCategorywiseTurnoverMap.toBson(bseClientCategorywiseTurnover)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (bseClientCategorywiseTurnoverList : List[BSEClientCategorywiseTurnover]) = {
    val builder = collection.initializeOrderedBulkOperation
    bseClientCategorywiseTurnoverList map {
      case bseClientCategorywiseTurnover => builder.find(
        MongoDBObject("_id.tradeDate" -> bseClientCategorywiseTurnover._id.tradeDate)).upsert().update(
          new BasicDBObject("$set",BSEClientCategorywiseTurnoverMap.toBson(bseClientCategorywiseTurnover)))
    }
    builder.execute()
  }

  def findOne (key : BSEClientCategorywiseTurnoverKey) : Option[BSEClientCategorywiseTurnover] = {
    val doc = collection.findOne(MongoDBObject("_id.tradeDate" -> key.tradeDate)).getOrElse(return None)
    Some(BSEClientCategorywiseTurnoverMap.fromBsom(doc))
  }
}


case class RecordKey (x1:String, x2:String, x3:String, x4:String)
case class Record (_id : RecordKey, x : String)

object BSEClientCategorywiseTurnoverDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()

    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    val tradeDate = dtf.parseLocalDateTime("28-January-2015 15:45:00")

    val key = BSEClientCategorywiseTurnoverKey(tradeDate.toDate)
    val bseClientCategorywiseTurnover = BSEClientCategorywiseTurnover(key, 1621.35,1690.43,-69.07,6.07,9.28,-3.21,824.35,819.85,4.50,4.51,7.43,-2.92,0.78,1.20,-0.42,27.51,58.22,-30.71,2454.14,2358.32,95.82)

    val context = new MongoContext
    context.connect()

    val dao = new BSEClientCategorywiseTurnoverDAO(context.test_coll)

    dao.insert(bseClientCategorywiseTurnover)

    println(dao.findOne(key))
    val bseClientCategorywiseTurnoverUpdated = BSEClientCategorywiseTurnover(key, 0,1690.43,-69.07,6.07,9.28,-3.21,824.35,819.85,4.50,4.51,7.43,-2.92,0.78,1.20,-0.42,27.51,58.22,-30.71,2454.14,2358.32,95.82)
    dao.bulkUpdate(List(bseClientCategorywiseTurnoverUpdated))
    println(dao.findOne(key))

//    val record = Record(RecordKey("a", "b", "c", "d"), "z")
//
//    val builder = context.test_coll.initializeOrderedBulkOperation
//    import CaseClassToMapImplicits._
//    builder.find(MongoDBObject("_id.x1" -> record._id.x1,
//      "_id.x2" -> record._id.x2,
//      "_id.x3" -> record._id.x3,
//      "_id.x4" -> record._id.x4)).upsert().update(new BasicDBObject("$set", grater[Record].asDBObject(record)))
//    println(grater[Record].asDBObject(record))
//    println(MongoDBObject(record.toStringWithFields.filterKeys(_ != "_id").toList))
//    println(builder.execute())

  }
}
