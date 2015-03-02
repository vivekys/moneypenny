package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import com.moneypenny.model.{BSEEndOfDayStockPriceKey, BSEEndOfDayStockPrice}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat

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
        val scripCode = csvRecord.get(0).toLong
        val scripId = csvRecord.get(1)
        val scripName = csvRecord.get(2)
        val data = fetchDataForId(startDate, endDate, scripId)
        logger.info(s"$scripCode, $scripId, $scripName - $data")
        val dataParser = CSVParser.parse(data, CSVFormat.EXCEL.withHeader())
        println(dataParser.getHeaderMap)
        returnMap.put((scripCode, scripId, scripName), data)
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

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    val bseEndOfDayStockPriceList = data map {
      case (key, data) =>
        val (scripCode, scripId, scripName) = key
        CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
          csvRecord =>
            val dateStr = csvRecord.get("Date") + " 15:45:00"
            BSEEndOfDayStockPrice(BSEEndOfDayStockPriceKey(scripCode, scripId, scripName, dtf.parseLocalDateTime(dateStr).toDate),
              if (csvRecord.get("Open Price").length == 0)  0 else csvRecord.get("Open Price").toDouble,
              if (csvRecord.get("High Price").length == 0)  0 else csvRecord.get("High Price").toDouble,
              if (csvRecord.get("Low Price").length == 0)  0 else csvRecord.get("Low Price").toDouble,
              if (csvRecord.get("Close Price").length == 0)  0 else csvRecord.get("Close Price").toDouble,
              if (csvRecord.get("WAP").length == 0)  0 else csvRecord.get("WAP").toDouble,
              if (csvRecord.get("No.of Shares").length == 0)  0 else csvRecord.get("No.of Shares").toLong,
              if (csvRecord.get("No. of Trades").length == 0)  0 else csvRecord.get("No. of Trades").toLong,
              if (csvRecord.get("Total Turnover (Rs.)").length == 0)  0 else csvRecord.get("Total Turnover (Rs.)").toLong,
              if (csvRecord.get("Deliverable Quantity").length == 0)  0 else csvRecord.get("Deliverable Quantity").toLong,
              if (csvRecord.get("% Deli. Qty to Traded Qty").length == 0)  0 else csvRecord.get("% Deli. Qty to Traded Qty").toDouble,
              if (csvRecord.get("Spread High-Low").length == 0)  0 else csvRecord.get("Spread High-Low").toDouble,
              if (csvRecord.get("Spread Close-Open").length == 0)  0 else csvRecord.get("Spread Close-Open").toDouble
            )
        }
    } flatMap {
      case ar => ar.toList
    }
    println(bseEndOfDayStockPriceList)
  }
}