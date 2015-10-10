package fetcher.moneypenny.model

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vivek on 10/10/15.
 */
case class MutualFundHouseKey(keyName : String)

case class MutualFundHouse(_id : MutualFundHouseKey, mfName : String)

object MutualFundHouseMap {
  def toBson(mutualFundHouse : MutualFundHouse) = {
    grater[MutualFundHouse].asDBObject(mutualFundHouse)
  }

  def fromBsom(o: DBObject) : MutualFundHouse = {
    grater[MutualFundHouse].asObject(o)
  }
}

class MutualFundHouseDAO (collection : MongoCollection) {
  def insert(mutualFundHouse : MutualFundHouse) = {
    val doc = MutualFundHouseMap.toBson(mutualFundHouse)
    collection.insert(doc)
  }

  def bulkInsert (mutualFundHouse : List[MutualFundHouse]) = {
    val builder = collection.initializeUnorderedBulkOperation
    mutualFundHouse map {
      case mfHouse => builder.insert(MutualFundHouseMap.toBson(mfHouse))
    }
    builder.execute()
  }

  def update(mutualFundHouse : MutualFundHouse) = {
    val query = MongoDBObject("_id.keyName" -> mutualFundHouse._id.keyName)
    val doc = MutualFundHouseMap.toBson(mutualFundHouse)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (mutualFundHouse : List[MutualFundHouse]) = {
    val builder = collection.initializeUnorderedBulkOperation
    mutualFundHouse map {
      case mfHouse => builder.find(MongoDBObject("_id.keyName" -> mfHouse._id.keyName)).
        upsert().update(new BasicDBObject("$set",MutualFundHouseMap.toBson(mfHouse)))
    }
    builder.execute()
  }

  def findOne (key : MutualFundHouseKey) : Option[MutualFundHouse] = {
    val doc = collection.findOne(MongoDBObject("_id.keyName" -> key.keyName)).getOrElse(return None)
    Some(MutualFundHouseMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    (for (element <- doc) yield MutualFundHouseMap.fromBsom(element)) toList
  }
}
