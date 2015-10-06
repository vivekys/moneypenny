package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html.{HtmlPage, HtmlTable}
import com.gargoylesoftware.htmlunit.{BrowserVersion, ElementNotFoundException, NicelyResynchronizingAjaxController, WebClient}
import com.moneypenny.util.{DomUtil, ExtractFromTable}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.collection.mutable.LinkedHashMap

/**
 * Created by vives on 1/3/15.
 */
class MoneycontrolFinancialFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  private val config = ConfigFactory.load

  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def hasData (page : HtmlPage) = {
    page != null && !page.asText().contains("Data Not Available for")
  }

  def fetchData (financialType : String, page : HtmlPage) = {
    var currentPage = page
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[String, Map[String, Option[Number]]]
    do {
      val finType = if (financialType.contains("Nine Monthly Results")) "Nine Months" else financialType
      val siblingTable = DomUtil.findTableElement(currentPage, finType).get
      var nextSibling = siblingTable.getNextSibling
      while (!nextSibling.isInstanceOf[HtmlTable]) {
        nextSibling = nextSibling.getNextSibling
      }
      val htmlTable = nextSibling.asInstanceOf[HtmlTable]
      returnMap ++= ExtractFromTable.extractFromHtmlTable(htmlTable)
      val anchor = try {
        currentPage.getAnchorByText("Previous Years »")
      } catch {
        case x : ElementNotFoundException => null
      }

      if (anchor != null) {
        currentPage = anchor.click().asInstanceOf[HtmlPage]
      }
      else
        currentPage = null
    } while (hasData(currentPage))
    logger.debug(returnMap.toString)
    returnMap

  }

  def fetchStandaloneData (financialType : String, url : String) = {
    logger.info(s"Extracting fetchStandaloneData - $url")
    fetchData(financialType, webClient.getPage(url).asInstanceOf[HtmlPage])
  }

  def fetchConsolidatedData (financialType : String, url : String) = {
    logger.info(s"Extracting fetchConsolidatedData - $url")
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    val consolidatedPageAnchor = page.getAnchorByText("Consolidated")
    val consolidatedPage = webClient.getPage(page.getFullyQualifiedUrl(consolidatedPageAnchor.getHrefAttribute)).asInstanceOf[HtmlPage]
    fetchData(financialType, consolidatedPage)
  }

  def fetch (financialType : String, url : String) = {
    logger.info(s"Extracting $financialType - $url")
    val returnMap = LinkedHashMap.empty[String, LinkedHashMap[String, Map[String, Option[Number]]]]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    if (page.asText().contains("Standalone"))
      returnMap.put(financialType + "-Standalone", fetchStandaloneData(financialType, url))
    if (page.asText().contains("Consolidated"))
      returnMap.put(financialType + "-Consolidated", fetchConsolidatedData(financialType, url))
    returnMap
  }

  def fetchAllFinancialData (url : String) = {
    val returnMap = LinkedHashMap.empty[String, LinkedHashMap[String, Map[String, Option[Number]]]]
    val finList = MoneycontrolFinListFetcher.fetchFinURLs(url)
    for ((k,v) <- finList) {
      if (!k.contains("Capital Structure"))
        returnMap ++= fetch(k, v)
    }
    returnMap
  }

}

object MoneycontrolFinancialFetcher {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")


    val mcBalSheetFetcher = new MoneycontrolFinancialFetcher
    val returnMap = mcBalSheetFetcher.fetchAllFinancialData("http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01")
    println(returnMap)
//    println(mcBalSheetFetcher.fetchStandaloneData("http://www.moneycontrol.com/financials/akcapitalservices/results/quarterly-results/AKC01#AKC01"))
  }
}