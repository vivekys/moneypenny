package fetcher.moneypenny.model

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vivek on 05/10/15.
 */

case class YahooAdjustedClosePriceStatsKey (runDate : Date, scripName : String)

case class YahooAdjustedClosePriceStats (_id : YahooAdjustedClosePriceStatsKey)


object YahooAdjustedClosePriceStatsMap {
  def toBson(yahooAdjustedClosePriceStats : YahooAdjustedClosePriceStats) = {
    grater[YahooAdjustedClosePriceStats].asDBObject(yahooAdjustedClosePriceStats)
  }

  def fromBsom(o: DBObject) : YahooAdjustedClosePriceStats = {
    grater[YahooAdjustedClosePriceStats].asObject(o)
  }
}

class YahooAdjustedClosePriceStatsDAO (collection : MongoCollection) {
  def insert(yahooAdjustedClosePriceStats : YahooAdjustedClosePriceStats) = {
    val doc = YahooAdjustedClosePriceStatsMap.toBson(yahooAdjustedClosePriceStats)
    collection.insert(doc)
  }

  def bulkInsert (yahooAdjustedClosePriceStats : List[YahooAdjustedClosePriceStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    yahooAdjustedClosePriceStats map {
      case yAdjustedClosePriceStats => builder.insert(YahooAdjustedClosePriceStatsMap.toBson(yAdjustedClosePriceStats))
    }
    builder.execute()
  }

  def update(yahooAdjustedClosePriceStats : YahooAdjustedClosePriceStats) = {
    val query = MongoDBObject("_id.runDate" -> yahooAdjustedClosePriceStats._id.runDate,
                              "_id.scripName" -> yahooAdjustedClosePriceStats._id.scripName)
    val doc = YahooAdjustedClosePriceStatsMap.toBson(yahooAdjustedClosePriceStats)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (yahooAdjustedClosePriceStats : List[YahooAdjustedClosePriceStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    yahooAdjustedClosePriceStats map {
      case yahooAdjustedClosePriceStats => builder.find(MongoDBObject(
        "_id.runDate" -> yahooAdjustedClosePriceStats._id.runDate,
        "_id.scripName" -> yahooAdjustedClosePriceStats._id.scripName)).
        upsert().update(
          new BasicDBObject("$set",YahooAdjustedClosePriceStatsMap.toBson(yahooAdjustedClosePriceStats)))
    }
    builder.execute()
  }

  def findOne (key : YahooAdjustedClosePriceStatsKey) : Option[YahooAdjustedClosePriceStats] = {
    val doc = collection.findOne(MongoDBObject("_id.runDate" -> key.runDate,
      "_id.scripName" -> key.scripName)).getOrElse(return None)
    Some(YahooAdjustedClosePriceStatsMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    (for (element <- doc) yield YahooAdjustedClosePriceStatsMap.fromBsom(element)) toList
  }
}