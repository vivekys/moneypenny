package com.moneypenny.model

import java.util.Date

import com.moneypenny.db.MongoContext
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat._
import com.novus.salat.global._

/**
 * Created by vives on 2/16/15.
 */
case class FinancialsKey (companyName : String, financial : String, financialType : String, fyDate : Date)

case class Financials (_id : FinancialsKey, data : Map[String, Option[Number]])

object FinancialsMap {
  def toBson(financials : Financials) = {
    grater[Financials].asDBObject(financials)
  }

  def fromBsom(o: DBObject) : Financials = {
    grater[Financials].asObject(o)
  }
}

class FinancialsDAO (collection : MongoCollection) {
  def insert(financials : Financials) = {
    val doc = FinancialsMap.toBson(financials)
    collection.insert(doc)
  }

  def bulkInsert (financialsList : List[Financials]) = {
    val builder = collection.initializeOrderedBulkOperation
    financialsList map {
      case financials => builder.insert(FinancialsMap.toBson(financials))
    }
    builder.execute()
  }

  def update(financials : Financials) = {
    val query = MongoDBObject("_id.companyName" -> financials._id.companyName,
      "_id.financial" -> financials._id.financial,
      "_id.financialType" -> financials._id.financialType,
      "_id.fyDate" -> financials._id.fyDate)
    val doc = FinancialsMap.toBson(financials)
    collection.update(query, doc, upsert=true)
  }

  def bulkUpdate (financialsList : List[Financials]) = {
    val builder = collection.initializeUnorderedBulkOperation
    financialsList map {
      case financials => builder.find(MongoDBObject("_id.companyName" -> financials._id.companyName,
        "_id.financial" -> financials._id.financial,
        "_id.financialType" -> financials._id.financialType,
        "_id.fyDate" -> financials._id.fyDate)).upsert().update(
          new BasicDBObject("$set", FinancialsMap.toBson(financials)))
    }
    builder.execute()
  }

  def findOne (key : FinancialsKey) : Option[Financials] = {
    val doc = collection.findOne(MongoDBObject("_id.companyName" -> key.companyName,
      "_id.financial" -> key.financial,
      "_id.financialType" -> key.financialType,
      "_id.fyDate" -> key.fyDate)).getOrElse(return None)
    Some(FinancialsMap.fromBsom(doc))
  }

  def findAll = {
    val doc = collection.find()
    for (element <- doc) yield FinancialsMap.fromBsom(element)
  }
}

object FinancialsDAOUtil {
  def getFinancialCollection (financial : String) = {
    val context = new MongoContext
    context.connect()
    financial match {
      case "Ratios" => context.ratiosCollection
      case "Quarterly Results" => context.quarterlyResultsCollection
      case "Balance Sheet" => context.balanceSheetCollection
      case "Yearly Results" => context.yearlyResultsCollection
      case "Nine Monthly Results" => context.nineMonthResultsCollection
      case "Profit & Loss" => context.profitAndLossCollection
      case "Cash Flow" => context.cashFlowCollection
      case "Half Yearly Results" => context.halfYearlyResultsCollection
    }
  }

  def getFinancialStatsCollection (financial : String) = {
    val context = new MongoContext
    context.connect()
    financial match {
      case "Ratios" => context.ratiosStatsCollection
      case "Quarterly Results" => context.quarterlyResultsStatsCollection
      case "Balance Sheet" => context.balanceSheetStatsCollection
      case "Yearly Results" => context.yearlyResultsStatsCollection
      case "Nine Monthly Results" => context.nineMonthResultsStatsCollection
      case "Profit & Loss" => context.profitAndLossStatsCollection
      case "Cash Flow" => context.cashFlowStatsCollection
      case "Half Yearly Results" => context.halfYearlyResultsStatsCollection
    }
  }
}
object FinancialsDAOTest {
  def main (args: Array[String]) {
    import com.mongodb.casbah.commons.conversions.scala._
    RegisterConversionHelpers()

    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")
  }
}

