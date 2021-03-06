package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import com.moneypenny.model.{BSEClientCategorywiseTurnover, BSEClientCategorywiseTurnoverKey}
import com.moneypenny.util.RetryFunExecutor
import fetcher.moneypenny.util.WebClientFactory
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vives on 1/1/15.
 *
 * Always fetch from the beginning of time till current date
 */
class BSEClientCategorywiseTurnoverFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val webClient = WebClientFactory.getWebClient

  def fetch (startDate : String, endDate : String) = {
    logger.info(s"Fetching BSEClientCategorywiseTurnover from $startDate to $endDate")

    try {
      RetryFunExecutor.retry(3) {
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
    } catch {
      case ex : Exception => logger.error(s"Error while Fetching BSEClientCategorywiseTurnover from $startDate to $endDate", ex)
        throw ex
    }
  }
}

object BSEClientCategorywiseTurnoverFetcher {
  def main (args: Array[String]) {
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val clientCategorywiseTurnoverFetcher = new BSEClientCategorywiseTurnoverFetcher
    val data = clientCategorywiseTurnoverFetcher.fetch("31/12/2014", "31/12/2014")

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
      case csvRecord => {
        val dateStr = csvRecord.get("Trade Date") + " 15:45:00"
        val bseClientCategorywiseTurnoverKey = new BSEClientCategorywiseTurnoverKey(dtf.parseLocalDateTime(dateStr).toDate)
        val bseClientCategorywiseTurnover = new BSEClientCategorywiseTurnover(bseClientCategorywiseTurnoverKey,
          if (csvRecord.get("Clients Buy").length == 0)  0 else csvRecord.get("Clients Buy").toDouble,
          if (csvRecord.get("Clients Sales").length == 0)  0 else csvRecord.get("Clients Sales").toDouble,
          if (csvRecord.get("Clients Net").length == 0)  0 else csvRecord.get("Clients Net").toDouble,
          if (csvRecord.get("NRI Buy").length == 0)  0 else csvRecord.get("NRI Buy").toDouble,
          if (csvRecord.get("NRI Sales").length == 0)  0 else csvRecord.get("NRI Sales").toDouble,
          if (csvRecord.get("NRI Net").length == 0)  0 else csvRecord.get("NRI Net").toDouble,
          if (csvRecord.get("Proprietary Buy").length == 0)  0 else csvRecord.get("Proprietary Buy").toDouble,
          if (csvRecord.get("Proprietary Sales").length == 0)  0 else csvRecord.get("Proprietary Sales").toDouble,
          if (csvRecord.get("Proprietary Net").length == 0)  0 else csvRecord.get("Proprietary Net").toDouble,
          if (csvRecord.get("IFIs Buy").length == 0)  0 else csvRecord.get("IFIs Buy").toDouble,
          if (csvRecord.get("IFIs Sales").length == 0)  0 else csvRecord.get("IFIs Sales").toDouble,
          if (csvRecord.get("IFIs Net").length == 0)  0 else csvRecord.get("IFIs Net").toDouble,
          if (csvRecord.get("Banks Buy").length == 0)  0 else csvRecord.get("Banks Buy").toDouble,
          if (csvRecord.get("Banks Sales").length == 0)  0 else csvRecord.get("Banks Sales").toDouble,
          if (csvRecord.get("Banks Net").length == 0)  0 else csvRecord.get("Banks Net").toDouble,
          if (csvRecord.get("Insurance Buy").length == 0)  0 else csvRecord.get("Insurance Buy").toDouble,
          if (csvRecord.get("Insurance Sales").length == 0)  0 else csvRecord.get("Insurance Sales").toDouble,
          if (csvRecord.get("Insurance Net").length == 0)  0 else csvRecord.get("Insurance Net").toDouble,
          if (csvRecord.get("DII(BSE + NSE + MCX-SX) Buy").length == 0)  0 else csvRecord.get("DII(BSE + NSE + MCX-SX) Buy").toDouble,
          if (csvRecord.get("DII(BSE + NSE + MCX-SX) Sales").length == 0)  0 else csvRecord.get("DII(BSE + NSE + MCX-SX) Sales").toDouble,
          if (csvRecord.get("DII(BSE + NSE + MCX-SX) Net").length == 0)  0 else csvRecord.get("DII(BSE + NSE + MCX-SX) Net").toDouble)
        println(bseClientCategorywiseTurnover)
      }
    }
  }
}