package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{Page, NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

/**
 * Created by vives on 2/7/15.
 */
class BSECorporateAction {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchListOfScrips = {
    val bseListOfScripsFetcher = new BSEListOfScripsFetcher
    bseListOfScripsFetcher.fetch
  }

  def fetchCAForId (startDate : String, endDate : String, id : String) = {
    logger.info(s"Fetching CA for $id from $startDate till $endDate")
    val page = webClient.getPage("http://www.bseindia.com/corporates/corporate_act.aspx?expandable=0").asInstanceOf[HtmlPage]

    val searchInput = page.getElementByName("ctl00$ContentPlaceHolder1$GetQuote1_smartSearch").asInstanceOf[HtmlTextInput]
    searchInput.`type`(id)
    val list = page.getElementById("listEQ").asInstanceOf[HtmlUnorderedList]
    val element = list.getElementsByTagName("a").get(0).asInstanceOf[HtmlAnchor]
    element.click()

    val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtDate").asInstanceOf[HtmlInput]
    fromDate.setValueAttribute(startDate)

    val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtTodate").asInstanceOf[HtmlInput]
    toDate.setValueAttribute(endDate)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val content = if (!newPage.asText.contains("No Corporate Actions During Selected Period")) {
      newPage.getElementById("ctl00_ContentPlaceHolder1_lnkDownload1").
        asInstanceOf[HtmlAnchor].click.asInstanceOf[Page].getWebResponse.getContentAsString
    } else
      ""
    content
  }

  def fetch (startDate : String, endDate : String) = {
    val returnMap = scala.collection.mutable.Map.empty[(Long, String, String), String]

    val list = fetchListOfScrips
    val csvParser = CSVParser.parse(list, CSVFormat.EXCEL.withHeader())
    for (csvRecord <- csvParser.getRecords) {
      if (csvRecord.get("Status") != "Delisted" && csvRecord.get("Status") != "N") {
        val scrip = csvRecord.get(1)
        val data = fetchCAForId(startDate, endDate, scrip)
        logger.info(s"CA for $scrip from $startDate to $endDate are")
        val (key, value) = ((csvRecord.get(0).toLong, csvRecord.get(1), csvRecord.get(2)), data)
        logger.info(s"key - $key")
        logger.info(s"value - $value")
        returnMap.put((csvRecord.get(0).toLong, csvRecord.get(1), csvRecord.get(2)), data)
      }
    }

    returnMap
  }
}


object BSECorporateAction {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseCorporateAction = new BSECorporateAction
    val data = bseCorporateAction.fetch("01/01/1990", "28/01/2015")
    println(data)
  }
}
