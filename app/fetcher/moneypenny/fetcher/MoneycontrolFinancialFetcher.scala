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
    page != null && !page.asText().contains("Data Not Available for Balance Sheet")
  }

  def fetchData (financialType : String, page : HtmlPage) = {
    var currentPage = page
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[(String, String), String]
    do {
      val siblingTable = DomUtil.findTableElement(page, financialType).get
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
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[(String, String), String]
    fetchData(financialType, webClient.getPage(url).asInstanceOf[HtmlPage])
  }

  def fetchConsolidatedData (financialType : String, url : String) = {
    logger.info(s"Extracting fetchConsolidatedData - $url")
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[(String, String), String]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    val consolidatedPageAnchor = page.getAnchorByText("Consolidated")
    val consolidatedPage = webClient.getPage(page.getFullyQualifiedUrl(consolidatedPageAnchor.getHrefAttribute)).asInstanceOf[HtmlPage]
    fetchData(financialType, consolidatedPage)
  }

  def fetch (financialType : String, url : String) = {
    logger.info(s"Extracting $financialType - $url")
    val returnMap = LinkedHashMap.empty[String, LinkedHashMap[(String, String), String]]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    if (page.asText().contains("Standalone"))
      returnMap.put(financialType + "-Standalone", fetchStandaloneData(financialType, url))
    if (page.asText().contains("Consolidated"))
      returnMap.put(financialType + "-Consolidated", fetchConsolidatedData(financialType, url))
    returnMap
  }
}

object MoneycontrolFinancialFetcher {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val returnMap = LinkedHashMap.empty[String, LinkedHashMap[(String, String), String]]
    val mcBalSheetFetcher = new MoneycontrolFinancialFetcher
    val finList = MoneycontrolFinListFetcher.fetchFinURLs("http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01")

//    mcBalSheetFetcher.fetch("Nine Monthly Results", "http://www.moneycontrol.com/financials/akcapitalservices/results/nine-months/AKC01#AKC01")
    for ((k,v) <- finList) {
      if (!k.contains("Capital Structure") && !k.contains("Nine Monthly Results"))
      returnMap ++= mcBalSheetFetcher.fetch(k, v)
    }
    println(returnMap)
//    println(mcBalSheetFetcher.fetchStandaloneData("http://www.moneycontrol.com/financials/akcapitalservices/results/quarterly-results/AKC01#AKC01"))
  }
}