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
case class YahooAdjustedClosePriceKey (symbol : String, tradeDate : Date)

case class YahooAdjustedClosePrice(_id : YahooAdjustedClosePriceKey, scripName : String, exchange : String,
                                   openPrice : Double,
                                   highPrice : Double,
                                   lowPrice : Double,
                                   closePrice : Double,
                                   numShares : Long,
                                   adjClosePrice : Double)

object YahooAdjustedClosePriceMap {
  def toBson(yahooAdjustedClosePrice : YahooAdjustedClosePrice) = {
    grater[YahooAdjustedClosePrice].asDBObject(yahooAdjustedClosePrice)
  }

  def fromBsom(o: DBObject) : YahooAdjustedClosePrice = {
    grater[YahooAdjustedClosePrice].asObject(o)
  }
}

class YahooAdjustedClosePriceDAO (collection : MongoCollection) {
  def insert(yahooAdjustedClosePrice : YahooAdjustedClosePrice) = {
    val doc = YahooAdjustedClosePriceMap.toBson(yahooAdjustedClosePrice)
    collection.insert(doc)
  }

  def bulkInsert (yahooAdjustedClosePrice : List[YahooAdjustedClosePrice]) = {
    val builder = collection.initializeUnorderedBulkOperation
    yahooAdjustedClosePrice map {
      case yAdjustedClosePrice => builder.insert(YahooAdjustedClosePriceMap.toBson(yAdjustedClosePrice))
    }
    builder.execute()
  }

  def update(yahooAdjustedClosePrice : YahooAdjustedClosePrice) = {
    val query = MongoDBObject("_id.symbol" -> yahooAdjustedClosePrice._id.symbol,
                              "_id.tradeDate" -> yahooAdjustedClosePrice._id.tradeDate)
    val doc = YahooAdjustedClosePriceMap.toBson(yahooAdjustedClosePrice)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (yahooAdjustedClosePrice : List[YahooAdjustedClosePrice]) = {
    val builder = collection.initializeOrderedBulkOperation
    yahooAdjustedClosePrice map {
      case yAdjustedClosePrice => builder.find(MongoDBObject("_id.symbol" -> yAdjustedClosePrice._id.symbol,
                                                             "_id.tradeDate" -> yAdjustedClosePrice._id.tradeDate)).
        upsert().update(
          new BasicDBObject("$set",YahooAdjustedClosePriceMap.toBson(yAdjustedClosePrice)))
    }
    builder.execute()
  }

  def findOne (key : YahooAdjustedClosePriceKey) : Option[YahooAdjustedClosePrice] = {
    val doc = collection.findOne(MongoDBObject("_id.symbol" -> key.symbol,
                                               "_id.tradeDate" -> key.tradeDate)).getOrElse(return None)
    Some(YahooAdjustedClosePriceMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    (for (element <- doc) yield YahooAdjustedClosePriceMap.fromBsom(element)) toList
  }
}