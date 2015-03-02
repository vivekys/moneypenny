package com.moneypenny.fetcher

import java.io.BufferedInputStream

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, Page, WebClient}
import com.typesafe.config.ConfigFactory
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

/**
 * Created by vives on 1/1/15.
 */

//Redundent data

class BSEGrossDeliverablesFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  private val config = ConfigFactory.load
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchListOfScrips = {
    val bseListOfScripsFetcher = new BSEListOfScripsFetcher
    bseListOfScripsFetcher.fetch
  }

  def fetchDataForId (startDate : String, endDate : String, id : String) = {
    logger.info(s"Fetching data for $id from $startDate till $endDate")
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
    logger.info(s"Fetching BSEGrossDeliverables from $startDate to $endDate")
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

  def fetch (year : String, month : String, day : String) = {
    logger.info(s"Fetching BSEGrossDeliverables for $year/$month/$day")
    val urlPattern = config.getString("com.moneypenny.xpath.BSEGrossDeliverablesFetcher")
    val url = urlPattern.replace("YEAR", year).replace("DAY", day).replace("MONTH", month)
    logger.info(s"Fetching BSEGrossDeliverables from $url")

    val is = webClient.getPage(url).asInstanceOf[Page].getWebResponse.getContentAsStream
    val zip = new ZipArchiveInputStream(new BufferedInputStream(is))
    zip.getNextEntry

    IOUtils.toString(zip, "UTF-8")
  }
}

object BSEGrossDeliverablesFetcher {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseGrossDeliverablesFetcher = new BSEGrossDeliverablesFetcher
    val data = bseGrossDeliverablesFetcher.fetch("2015", "01", "28")
    println(data)
  }
}