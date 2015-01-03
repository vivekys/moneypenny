package com.moneypenny.fetcher

import java.net.URL

import com.gargoylesoftware.htmlunit.html.{HtmlAnchor, HtmlTable, HtmlPage}
import com.gargoylesoftware.htmlunit.{ElementNotFoundException, NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import com.moneypenny.util.ExtractFromTable
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import scala.collection.mutable.LinkedHashMap

/**
 * Created by vives on 1/3/15.
 */
class MoneycontrolFinancialFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def hasData (page : HtmlPage) = {
    page != null && !page.asText().contains("Data Not Available for Balance Sheet")
  }

  def fetchData (page : HtmlPage) = {
    var currentPage = page
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[(String, String), String]
    do {
      val standaloneHtmlTable = currentPage.getByXPath("/html/body/center[2]/div/div[1]/div[8]/div[3]/div[2]/div[2]/div/div[1]/table[3]").
        get(0).asInstanceOf[HtmlTable]
      returnMap ++= ExtractFromTable.extractFromHtmlTable(standaloneHtmlTable)
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
    println(returnMap)
    returnMap

  }

  def fetchStandaloneData (url : String) = {
    println(s"Extracting fetchStandaloneData - $url")
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[(String, String), String]
    fetchData(webClient.getPage(url).asInstanceOf[HtmlPage])
  }

  def fetchConsolidatedData (url : String) = {
    println(s"Extracting fetchConsolidatedData - $url")
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[(String, String), String]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    val consolidatedPageAnchor = page.getAnchorByText("Consolidated")
    val consolidatedPage = webClient.getPage(page.getFullyQualifiedUrl(consolidatedPageAnchor.getHrefAttribute)).asInstanceOf[HtmlPage]
    fetchData(consolidatedPage)
  }

  def fetch (financialType : String, url : String) = {
    println(s"Extracting $financialType - $url")
    val returnMap = LinkedHashMap.empty[String, LinkedHashMap[(String, String), String]]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    if (page.asText().contains("Standalone"))
      returnMap.put(financialType + "-Standalone", fetchStandaloneData(url))
    if (page.asText().contains("Consolidated"))
      returnMap.put(financialType + "-Consolidated", fetchConsolidatedData(url))
    returnMap
  }
}

object MoneycontrolFinancialFetcher {
  def main (args: Array[String]) {
    val returnMap = LinkedHashMap.empty[String, LinkedHashMap[(String, String), String]]
    val mcBalSheetFetcher = new MoneycontrolFinancialFetcher
    val finList = MoneycontrolFinListFetcher.fetchFinURLs("http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01")

    for ((k,v) <- finList) {
      if (k != "Capital Structure")
      returnMap ++= mcBalSheetFetcher.fetch(k, v)
    }
    println(returnMap)
//    println(mcBalSheetFetcher.fetchStandaloneData("http://www.moneycontrol.com/financials/akcapitalservices/results/quarterly-results/AKC01#AKC01"))
  }
}