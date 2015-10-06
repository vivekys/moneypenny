package fetcher.moneypenny.model

/**
 * Created by vivek on 04/10/15.
 */

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

case class YahooListOfScripsKey (symbol : String)

case class YahooListOfScrips (_id : YahooListOfScripsKey, scripName : String, exchange : String)

object YahooListOfScripsMap {
  def toBson(yahooListOfScrips : YahooListOfScrips) = {
    grater[YahooListOfScrips].asDBObject(yahooListOfScrips)
  }

  def fromBsom(o: DBObject) : YahooListOfScrips = {
    grater[YahooListOfScrips].asObject(o)
  }
}

class YahooListOfScripsDAO (collection : MongoCollection) {
  def insert(yahooListOfScrips : YahooListOfScrips) = {
    val doc = YahooListOfScripsMap.toBson(yahooListOfScrips)
    collection.insert(doc)
  }

  def bulkInsert (yahooListOfScrips : List[YahooListOfScrips]) = {
    val builder = collection.initializeUnorderedBulkOperation
    yahooListOfScrips map {
      case yahooListOfScrip => builder.insert(YahooListOfScripsMap.toBson(yahooListOfScrip))
    }
    builder.execute()
  }

  def update(yahooListOfScrips : YahooListOfScrips) = {
    val query = MongoDBObject("_id.symbol" -> yahooListOfScrips._id.symbol)
    val doc = YahooListOfScripsMap.toBson(yahooListOfScrips)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (yahooListOfScrips : List[YahooListOfScrips]) = {
    val builder = collection.initializeUnorderedBulkOperation
    yahooListOfScrips map {
      case yahooListOfScrip => builder.find(MongoDBObject("_id.symbol" -> yahooListOfScrip._id.symbol)).
        upsert().update(
          new BasicDBObject("$set",YahooListOfScripsMap.toBson(yahooListOfScrip)))
    }
    builder.execute()
  }

  def findOne (key : YahooListOfScripsKey) : Option[YahooListOfScrips] = {
    val doc = collection.findOne(MongoDBObject("_id.symbol" -> key.symbol)).getOrElse(return None)
    Some(YahooListOfScripsMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    (for (element <- doc) yield YahooListOfScripsMap.fromBsom(element)) toList
  }
}
