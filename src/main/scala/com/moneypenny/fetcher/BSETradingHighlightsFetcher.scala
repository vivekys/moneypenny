package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

/**
 * Created by vives on 1/1/15.
 */
class BSETradingHighlightsFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetch (startDate : String, endDate : String) = {
    val page = webClient.getPage("http://www.bseindia.com/markets/Equity/EQReports/Tradinghighlights_histroical.aspx?expandable=7").asInstanceOf[HtmlPage]

    val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtDate").asInstanceOf[HtmlInput]
    fromDate.setValueAttribute(startDate)

    val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtToDate").asInstanceOf[HtmlInput]
    toDate.setValueAttribute(endDate)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val content = newPage.getElementByName("ctl00$ContentPlaceHolder1$imgDownload1").
      asInstanceOf[HtmlImageInput].click.getWebResponse.getContentAsString
    content
  }
}

object BSETradingHighlightsFetcher {
  def main (args: Array[String]) {
    val bseTradingHighlightsFetcher = new BSETradingHighlightsFetcher
    val data = bseTradingHighlightsFetcher.fetch("01/01/1990", "01/01/2015")
    println(data)
  }
}