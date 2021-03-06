package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import com.moneypenny.model.{BSEEndOfDayStockPrice, BSEEndOfDayStockPriceKey, BSEListOfScrips}
import com.moneypenny.util.RetryFunExecutor
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
/**
 * Created by vives on 1/1/15.
 */
class BSEEndOfDayStockPriceFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def fetchListOfScrips = {
    val bSEListOfScripsFetcher = new BSEListOfScripsFetcher
    bSEListOfScripsFetcher.fetchListOfScrips
  }

  def fetch (startDate : String, endDate : String, scripCode : Long, scripId : String, scripName : String) :
                                                        scala.collection.mutable.Map[(Long, String, String), String] = {
    logger.info(s"Fetching data for $scripId from $startDate till $endDate")
    val returnMap = scala.collection.mutable.Map.empty[(Long, String, String), String]
    
    try {
      RetryFunExecutor.retry(3) {
        val webClient = new WebClient(BrowserVersion.CHROME)
        webClient.getOptions().setThrowExceptionOnScriptError(false)
        webClient.setAjaxController(new NicelyResynchronizingAjaxController())

        val page = webClient.getPage("http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&flag=0").asInstanceOf[HtmlPage]
        val equityRadioButton = page.getElementById("ctl00_ContentPlaceHolder1_rad_no1").asInstanceOf[HtmlRadioButtonInput]
        equityRadioButton.setChecked(true)

        val searchInput = page.getElementByName("ctl00$ContentPlaceHolder1$GetQuote1_smartSearch").asInstanceOf[HtmlTextInput]
        searchInput.`type`(scripId)
        val list = page.getElementById("listEQ").asInstanceOf[HtmlUnorderedList]
        if (list != null && list.getElementsByTagName("a").length != 0) {
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
          val data = newPage.getElementByName("ctl00$ContentPlaceHolder1$btnDownload").
            asInstanceOf[HtmlImageInput].click.getWebResponse.getContentAsString
          returnMap.put((scripCode, scripId, scripName), data)
        }
      }
    } catch {
      case ex : Exception => logger.info(s"Error while Fetching data for $scripId from $startDate to $endDate", ex)
    }
    returnMap
  }

  def fetch (startDate : String, endDate : String) : scala.collection.mutable.Map[(Long, String, String), String] = {
    val returnMap = scala.collection.mutable.Map.empty[(Long, String, String), String]

    val bseListOfScrips = fetchListOfScrips
    val countOfScrips = bseListOfScrips.length
    logger.info(s"Fetching end of data stock price for $countOfScrips stocks b/w $startDate - $endDate")
    bseListOfScrips.par .filter((bseScrip : BSEListOfScrips) => bseScrip.status.get != "Delisted" &&
      bseScrip.status.get != "N") map {
      case bseScrip => {
        val scripCode = bseScrip._id.scripCode
        val scripId = bseScrip.scripId.get
        val scripName = bseScrip.scripName.get
        logger.info(s"$scripCode, $scripId, $scripName")
        try {
          fetch(startDate, endDate, scripCode, scripId, scripName) map {
            case (key, value) =>
              returnMap.put((key._1, key._2, key._3), value)
          }
        } catch {
          case ex : Exception =>
            logger.error(s"Error while fetching daily quote for $scripCode, $scripId, $scripName", ex)
        }
      }
    }
    returnMap
  }
}

object BSEEndOfDayStockPriceFetcher {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val bseEndOfDayStockPriceFetcher = new BSEEndOfDayStockPriceFetcher
    val data = bseEndOfDayStockPriceFetcher.fetch("01/01/1990", "06/03/2015")

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