package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import com.moneypenny.model.{BSETradingHighlights, BSETradingHighlightsKey}
import com.moneypenny.util.RetryFunExecutor
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat
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
    logger.info(s"Fetching BSETradingHighlights from $startDate to $endDate")
    try {
      RetryFunExecutor.retry(3) {
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
    } catch {
      case ex : Exception => logger.error(s"Error while Fetching BSETradingHighlights from $startDate to $endDate", ex)
        throw ex
    }
  }
}

object BSETradingHighlightsFetcher {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseTradingHighlightsFetcher = new BSETradingHighlightsFetcher
    val data = bseTradingHighlightsFetcher.fetch("01/01/2015", "13/02/2015")

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")
    CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
      csvRecord =>
        val dateStr = csvRecord.get("Date") + " 15:45:00"
        BSETradingHighlights(BSETradingHighlightsKey(dtf.parseLocalDateTime(dateStr).toDate),
          if (csvRecord.get("Scrips Traded	").length == 0)  0 else csvRecord.get("Scrips Traded	").toLong,
          if (csvRecord.get("No. of Trades").length == 0)  0 else csvRecord.get("No. of Trades").toLong,
          if (csvRecord.get("Traded Qty.(Cr.)").length == 0)  0 else csvRecord.get("Traded Qty.(Cr.)").toDouble,
          if (csvRecord.get("Total T/O (Cr.)").length == 0)  0 else csvRecord.get("Total T/O (Cr.)").toDouble,
          if (csvRecord.get("Advance").length == 0)  0 else csvRecord.get("Advance").toLong,
          if (csvRecord.get("Advances as % of Scrips Traded").length == 0)  0 else csvRecord.get("Advances as % of Scrips Traded").toDouble,
          if (csvRecord.get("Decline").length == 0)  0 else csvRecord.get("Decline").toLong,
          if (csvRecord.get("Declines as % of Scrips Traded	").length == 0)  0 else csvRecord.get("Declines as % of Scrips Traded	").toDouble,
          if (csvRecord.get("Unchanged").length == 0)  0 else csvRecord.get("Unchanged").toLong,
          if (csvRecord.get("Unchanged as % of Scrips Traded	").length == 0)  0 else csvRecord.get("Unchanged as % of Scrips Traded	").toDouble,
          if (csvRecord.get("Scrips on Upper Circuit").length == 0)  0 else csvRecord.get("Scrips on Upper Circuit").toLong,
          if (csvRecord.get("Scrips on Lower Circuit").length == 0)  0 else csvRecord.get("Scrips on Lower Circuit").toLong,
          if (csvRecord.get("Scrips Touching 52W H").length == 0)  0 else csvRecord.get("Scrips Touching 52W H").toLong,
          if (csvRecord.get("Scrips Touching 52W L").length == 0)  0 else csvRecord.get("Scrips Touching 52W L").toLong)
    }
    println(data)
  }
}