package fetcher.moneypenny.model

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vivek on 04/10/15.
 */
case class YahooListOfScripsStatsKey (runDate : Date)

case class YahooListOfScripsStats (_id : YahooListOfScripsStatsKey, letter : String)


object YahooListOfScripsStatsMap {
  def toBson(yahooListOfScripsStats : YahooListOfScripsStats) = {
    grater[YahooListOfScripsStats].asDBObject(yahooListOfScripsStats)
  }

  def fromBsom(o: DBObject) : YahooListOfScripsStats = {
    grater[YahooListOfScripsStats].asObject(o)
  }
}

class YahooListOfScripsStatsDAO (collection : MongoCollection) {
  def insert(yahooListOfScripsStats : YahooListOfScripsStats) = {
    val doc = YahooListOfScripsStatsMap.toBson(yahooListOfScripsStats)
    collection.insert(doc)
  }

  def bulkInsert (yahooListOfScripsStats : List[YahooListOfScripsStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    yahooListOfScripsStats map {
      case yahooListOfScrip => builder.insert(YahooListOfScripsStatsMap.toBson(yahooListOfScrip))
    }
    builder.execute()
  }

  def update(yahooListOfScripsStats : YahooListOfScripsStats) = {
    val query = MongoDBObject("_id.runDate" -> yahooListOfScripsStats._id.runDate)
    val doc = YahooListOfScripsStatsMap.toBson(yahooListOfScripsStats)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (yahooListOfScripsStats : List[YahooListOfScripsStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    yahooListOfScripsStats map {
      case yListOfScripsStats => builder.find(MongoDBObject("_id.runDate" -> yListOfScripsStats._id.runDate)).
        upsert().update(
          new BasicDBObject("$set",YahooListOfScripsStatsMap.toBson(yListOfScripsStats)))
    }
    builder.execute()
  }

  def findOne (key : YahooListOfScripsStatsKey) : Option[YahooListOfScripsStats] = {
    val doc = collection.findOne(MongoDBObject("_id.runDate" -> key.runDate)).getOrElse(return None)
    Some(YahooListOfScripsStatsMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    (for (element <- doc) yield YahooListOfScripsStatsMap.fromBsom(element)) toList
  }
}
