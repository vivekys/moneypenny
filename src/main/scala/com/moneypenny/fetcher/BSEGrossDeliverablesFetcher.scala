package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

/**
 * Created by vives on 1/1/15.
 */
class BSEGrossDeliverablesFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchListOfScrips = {
    val bseListOfScripsFetcher = new BSEListOfScripsFetcher
    bseListOfScripsFetcher.fetch
  }

  def fetchDataForId (startDate : String, endDate : String, id : String) = {
    logger.fatal(String.format("Fetching data for %s from %s till %s", id, startDate, endDate))
    val page = webClient.getPage("http://www.bseindia.com/markets/equity/EQReports/GrossDeliverables.aspx?expandable=7").asInstanceOf[HtmlPage]
    val equityRadioButton = page.getElementById("rbDate").asInstanceOf[HtmlRadioButtonInput]
    equityRadioButton.setChecked(true)
    equityRadioButton.click()

    val searchInput = page.getElementById("ctl00_ContentPlaceHolder1_GetQuote1_smartSearch").asInstanceOf[HtmlTextInput]
    searchInput.`type`(id)
    val list = page.getElementById("listEQ").asInstanceOf[HtmlUnorderedList]
    val element = list.getElementsByTagName("a").get(0).asInstanceOf[HtmlAnchor]
    element.click()

    val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtfromdate").asInstanceOf[HtmlInput]
    fromDate.setValueAttribute(startDate)

    val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtTodate").asInstanceOf[HtmlInput]
    toDate.setValueAttribute(endDate)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val content = newPage.getElementByName("ctl00$ContentPlaceHolder1$imgDownload1").
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

object BSEGrossDeliverablesFetcher {
  def main (args: Array[String]) {
    val bseGrossDeliverablesFetcher = new BSEGrossDeliverablesFetcher
    val data = bseGrossDeliverablesFetcher.fetch("31/12/2014", "31/12/2014")
    println(data)
  }
}