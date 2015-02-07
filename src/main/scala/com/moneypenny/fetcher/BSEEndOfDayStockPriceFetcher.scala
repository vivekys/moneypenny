package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
/**
 * Created by vives on 1/1/15.
 */
class BSEEndOfDayStockPriceFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchListOfScrips = {
    val bseListOfScripsFetcher = new BSEListOfScripsFetcher
    bseListOfScripsFetcher.fetch
  }

  def fetchDataForId (startDate : String, endDate : String, id : String) = {
    logger.info(s"Fetching data for $id from $startDate till $endDate")
    val page = webClient.getPage("http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&flag=0").asInstanceOf[HtmlPage]
    val equityRadioButton = page.getElementById("ctl00_ContentPlaceHolder1_rad_no1").asInstanceOf[HtmlRadioButtonInput]
    equityRadioButton.setChecked(true)

    val searchInput = page.getElementByName("ctl00$ContentPlaceHolder1$GetQuote1_smartSearch").asInstanceOf[HtmlTextInput]
    searchInput.`type`(id)
    val list = page.getElementById("listEQ").asInstanceOf[HtmlUnorderedList]
    val element = list.getElementsByTagName("a").get(0).asInstanceOf[HtmlAnchor]
    element.click()

    val dailyRadioButton = page.getElementById("ctl00_ContentPlaceHolder1_rdbDaily").asInstanceOf[HtmlRadioButtonInput]
    dailyRadioButton.setChecked(true)
    val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtFromDate").asInstanceOf[HtmlInput]
    fromDate.setValueAttribute(startDate)

    val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtToDate").asInstanceOf[HtmlInput]
    toDate.setValueAttribute(endDate)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val content = newPage.getElementByName("ctl00$ContentPlaceHolder1$btnDownload").
      asInstanceOf[HtmlImageInput].click.getWebResponse.getContentAsString
    content
  }

  def fetch (startDate : String, endDate : String) = {
    val returnMap = scala.collection.mutable.Map.empty[(Long, String, String), String]

    val list = fetchListOfScrips

    val csvParser = CSVParser.parse(list, CSVFormat.EXCEL.withHeader())
    for (csvRecord <- csvParser.getRecords) {
      if (csvRecord.get("Status") != "Delisted" && csvRecord.get("Status") != "N") {
        val data = fetchDataForId(startDate, endDate, csvRecord.get(1))
        returnMap.put((csvRecord.get(0).toLong, csvRecord.get(1), csvRecord.get(2)), data)
      }
    }
    returnMap
  }

}

object BSEEndOfDayStockPriceFetcher {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseEndOfDayStockPriceFetcher = new BSEEndOfDayStockPriceFetcher
    val data = bseEndOfDayStockPriceFetcher.fetch("28/01/2015", "28/01/2015")
    println(data)
  }
}