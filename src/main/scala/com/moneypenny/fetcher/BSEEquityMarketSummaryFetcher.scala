package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

/**
 * Created by vives on 1/1/15.
 */
class BSEEquityMarketSummaryFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchFromMonthAnchor (anchorText : String, page : HtmlPage) = {
    logger.info(s"Fetching from month anchor $anchorText")
    val anchor = page.getAnchorByText(anchorText).asInstanceOf[HtmlAnchor]
    val newPage = anchor.click().asInstanceOf[HtmlPage]
    val content = newPage.getElementByName("ctl00$ContentPlaceHolder1$btnDownload").
      asInstanceOf[HtmlImageInput].click.getWebResponse.getContentAsString
    content
  }

  def fetchFromYearAnchor (anchorText : String, page : HtmlPage) = {
    logger.info(s"Fetching from year anchor $anchorText")
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    val yearAnchor = page.getAnchorByText(anchorText).asInstanceOf[HtmlAnchor]
    val newTablePage = yearAnchor.click().asInstanceOf[HtmlPage]
    val htmlTable = newTablePage.getHtmlElementById("ctl00_ContentPlaceHolder1_gvYearwise").asInstanceOf[HtmlTable]

    for (i <- 1 until htmlTable.getRows.length) {
      val cell = htmlTable.getRows.get(i).getCell(0).asInstanceOf[HtmlTableCell]
      val monAnchor = cell.getElementsByTagName("a").get(0).asInstanceOf[HtmlAnchor]
      val refreshedPage = webClient.getPage("http://www.bseindia.com/markets/Equity/EQReports/Historical_EquitySegment.aspx?expandable=7").asInstanceOf[HtmlPage]
      val refreshedYearAnchor = refreshedPage.getAnchorByText(anchorText).asInstanceOf[HtmlAnchor]
      val refreshedTablePage = refreshedYearAnchor.click().asInstanceOf[HtmlPage]
      val data = fetchFromMonthAnchor(monAnchor.asText(), refreshedTablePage)
      logger.info("Key - " +  monAnchor.asText())
      logger.info(s"$data")
      returnMap.put(monAnchor.asText(), data)
    }
    returnMap
  }

  def fetch(latestOnly : Boolean) = {
    var finalMap = scala.collection.mutable.Map.empty[String, String]
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    val page = webClient.getPage("http://www.bseindia.com/markets/Equity/EQReports/Historical_EquitySegment.aspx?expandable=7").asInstanceOf[HtmlPage]
    val htmlTable = page.getHtmlElementById("ctl00_ContentPlaceHolder1_gvReport").asInstanceOf[HtmlTable]
    val len = if (latestOnly) 2 else htmlTable.getRows.length
    for (i <- 1 until len) {
      val cell = htmlTable.getRows.get(i).getCell(0).asInstanceOf[HtmlTableCell]
      val anchor = cell.getElementsByTagName("a").get(0).asInstanceOf[HtmlAnchor]
      if (anchor.getId.contains("month")) {
        val newPage = webClient.getPage("http://www.bseindia.com/markets/Equity/EQReports/Historical_EquitySegment.aspx?expandable=7").asInstanceOf[HtmlPage]
        val data = fetchFromMonthAnchor(anchor.asText(), newPage)
        logger.info("Key - " +  anchor.asText())
        logger.info(s"$data")
        returnMap.put(anchor.asText(), data)
      }
      else {
        val year = anchor.asText()
        val newPage = webClient.getPage("http://www.bseindia.com/markets/Equity/EQReports/Historical_EquitySegment.aspx?expandable=7").asInstanceOf[HtmlPage]
        val map = fetchFromYearAnchor(year, newPage)
        finalMap = finalMap ++ map
      }
    }
    finalMap ++ returnMap
  }
}

object BSEEquityMarketSummaryFetcher {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseEquityMarketSummaryFetcher = new BSEEquityMarketSummaryFetcher
    val data = bseEquityMarketSummaryFetcher.fetch(false)
    println(data)
  }
}