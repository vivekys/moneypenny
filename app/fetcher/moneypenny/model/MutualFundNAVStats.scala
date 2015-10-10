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
case class MutualFundNAVStatsKey (mfKey : MutualFundHouseKey, tp : String, runDate : Date)

case class MutualFundNAVStats (_id : MutualFundNAVStatsKey)

object MutualFundNAVStatsMap {
  def toBson(mutualFundNAVStats : MutualFundNAVStats) = {
    grater[MutualFundNAVStats].asDBObject(mutualFundNAVStats)
  }

  def fromBsom(o: DBObject) : MutualFundNAVStats = {
    grater[MutualFundNAVStats].asObject(o)
  }
}

class MutualFundNAVStatsDAO (collection : MongoCollection) {
  def insert(mutualFundNAVStats : MutualFundNAVStats) = {
    val doc = MutualFundNAVStatsMap.toBson(mutualFundNAVStats)
    collection.insert(doc)
  }

  def bulkInsert (mutualFundNAVStats : List[MutualFundNAVStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    mutualFundNAVStats map {
      case mfNAVStats => builder.insert(MutualFundNAVStatsMap.toBson(mfNAVStats))
    }
    builder.execute()
  }

  def update(mutualFundNAVStats : MutualFundNAVStats) = {
    val query = MongoDBObject("_id.mfKey" -> mutualFundNAVStats._id.mfKey,
              "_id.tp" -> mutualFundNAVStats._id.tp,
              "_id.runDate" -> mutualFundNAVStats._id.runDate)
    val doc = MutualFundNAVStatsMap.toBson(mutualFundNAVStats)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (mutualFundNAVStats : List[MutualFundNAVStats]) = {
    val builder = collection.initializeUnorderedBulkOperation
    mutualFundNAVStats map {
      case mfNAVStats => builder.find(MongoDBObject( "_id.mfKey" -> mfNAVStats._id.mfKey,
        "_id.tp" -> mfNAVStats._id.tp,
        "_id.runDate" -> mfNAVStats._id.runDate)).
        upsert().update(
        new BasicDBObject("$set",MutualFundNAVStatsMap.toBson(mfNAVStats)))
    }
    builder.execute()
  }

  def findOne (key : MutualFundNAVStatsKey) : Option[MutualFundNAVStats] = {
    val doc = collection.findOne(MongoDBObject( "_id.mfKey" -> key.mfKey,
      "_id.tp" -> key.tp, "_id.runDate" -> key.runDate)).getOrElse(return None)
    Some(MutualFundNAVStatsMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    (for (element <- doc) yield MutualFundNAVStatsMap.fromBsom(element)) toList
  }
}

