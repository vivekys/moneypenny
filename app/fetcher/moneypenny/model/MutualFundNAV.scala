package fetcher.moneypenny.model

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vivek on 10/10/15.
 */
case class MutualFundNAVKey (schemeCode : Long, tradeDate : Date)

case class MutualFundNAV (_id : MutualFundNAVKey, isinDivGrowth : String, isinDivReInvest : String,
                           schemeName : String, nav : Double, rePurchasePrice : Double, salePrice : Double)


object MutualFundNAVMap {
  def toBson(mutualFundNAV : MutualFundNAV) = {
    grater[MutualFundNAV].asDBObject(mutualFundNAV)
  }

  def fromBsom(o: DBObject) : MutualFundNAV = {
    grater[MutualFundNAV].asObject(o)
  }
}

class MutualFundNAVDAO (collection : MongoCollection) {
  def insert(mutualFundNAV : MutualFundNAV) = {
    val doc = MutualFundNAVMap.toBson(mutualFundNAV)
    collection.insert(doc)
  }

  def bulkInsert (mutualFundNAV : List[MutualFundNAV]) = {
    val builder = collection.initializeUnorderedBulkOperation
    mutualFundNAV map {
      case mfNAV => builder.insert(MutualFundNAVMap.toBson(mfNAV))
    }
    builder.execute()
  }

  def update(mutualFundNAV : MutualFundNAV) = {
    val query = MongoDBObject("_id.schemeCode" -> mutualFundNAV._id.schemeCode,
      "_id.tradeDate" -> mutualFundNAV._id.tradeDate)
    val doc = MutualFundNAVMap.toBson(mutualFundNAV)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (mutualFundNAV : List[MutualFundNAV]) = {
    val builder = collection.initializeOrderedBulkOperation
    mutualFundNAV map {
      case mfNAV => builder.find(MongoDBObject("_id.schemeCode" -> mfNAV._id.schemeCode,
        "_id.tradeDate" -> mfNAV._id.tradeDate)).
        upsert().update(
        new BasicDBObject("$set",MutualFundNAVMap.toBson(mfNAV)))
    }
    builder.execute()
  }

  def findOne (key : MutualFundNAVKey) : Option[MutualFundNAV] = {
    val doc = collection.findOne(MongoDBObject("_id.schemeCode" -> key.schemeCode,
      "_id.tradeDate" -> key.tradeDate)).getOrElse(return None)
    Some(MutualFundNAVMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    (for (element <- doc) yield MutualFundNAVMap.fromBsom(element)) toList
  }
}