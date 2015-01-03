package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

/**
 * Created by vives on 1/1/15.
 */
class BSEClientCategorywiseTurnoverFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetch (startDate : String, endDate : String) = {
    val page = webClient.getPage("http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&flag=1").asInstanceOf[HtmlPage]

    val dailyRadioButton = page.getElementById("ctl00_ContentPlaceHolder1_rdbDaily").asInstanceOf[HtmlRadioButtonInput]
    dailyRadioButton.setChecked(true)
    val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtFromDate").asInstanceOf[HtmlInput]
    fromDate.setValueAttribute(startDate)

    val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtToDate").asInstanceOf[HtmlInput]
    toDate.setValueAttribute(endDate)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val content = newPage.getElementByName("ctl00$ContentPlaceHolder1$btnDownload2").
      asInstanceOf[HtmlImageInput].click.getWebResponse.getContentAsString
    content
  }
}

object BSEClientCategorywiseTurnoverFetcher {
  def main (args: Array[String]) {
    val clientCategorywiseTurnoverFetcher = new BSEClientCategorywiseTurnoverFetcher
    val data = clientCategorywiseTurnoverFetcher.fetch("31/12/2014", "31/12/2014")
    println(data)
  }
}