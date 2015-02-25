package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import com.moneypenny.model.{BSEIndices, BSEIndicesKey}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConversions._


/**
 * Created by vives on 12/29/14.
 */
class BSEIndicesFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchOptions = {
    val page = webClient.getPage("http://www.bseindia.com/indices/IndexArchiveData.aspx?expandable=1").asInstanceOf[HtmlPage]
    val htmlTable = page.getHtmlElementById("DMY").asInstanceOf[HtmlTable]
    val htmlSelect = page.getElementByName("ctl00$ContentPlaceHolder1$ddlIndex").asInstanceOf[HtmlSelect]
    val len = htmlSelect.getOptions.length
    for (i <- 1 until len) yield htmlSelect.getOptions.get(i)


  }
  def fetchDataForOption (startDate : String, endDate : String, option : String) = {
    logger.info(s"Fetching $option from $startDate to $endDate")
    val page = webClient.getPage("http://www.bseindia.com/indices/IndexArchiveData.aspx?expandable=1").asInstanceOf[HtmlPage]
    val htmlTable = page.getHtmlElementById("DMY").asInstanceOf[HtmlTable]
    val htmlSelect = page.getElementByName("ctl00$ContentPlaceHolder1$ddlIndex").asInstanceOf[HtmlSelect]
    val opt = htmlSelect.getOptionByValue(option)
    htmlSelect.setSelectedAttribute(opt, true)

    val dailyRadioButton = page.getElementById("ctl00_ContentPlaceHolder1_rdbDaily").asInstanceOf[HtmlRadioButtonInput]
    dailyRadioButton.setChecked(true)
    val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtFromDate").asInstanceOf[HtmlInput]
    fromDate.setValueAttribute(startDate)

    val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtToDate").asInstanceOf[HtmlInput]
    toDate.setValueAttribute(endDate)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val content = newPage.getElementByName("ctl00$ContentPlaceHolder1$btnDownload1").
      asInstanceOf[HtmlImageInput].click.getWebResponse.getContentAsString
    (opt.getText, content)
  }

  def fetch (startDate : String, endDate : String) = {
    logger.info(s"Fetching Indices from $startDate to $endDate")
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    val options = fetchOptions
    for (opt <- options) {
      val kv = fetchDataForOption(startDate, endDate, opt.getValueAttribute)
      returnMap.put(kv._1, kv._2)
    }
    returnMap.toMap
  }
}

object BSEIndicesFetcher {
  def main (args: Array[String]) {
    val logger = Logger.getLogger(this.getClass.getSimpleName)
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseIndicesFetcher = new BSEIndicesFetcher
    val indices = bseIndicesFetcher.fetch("01/01/1990", "22/02/2015")

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")

    val bseIndicesList =  indices map {
        case (index, data) => CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
          csvRecord =>
            val dateStr = csvRecord.get("Date") + " 15:45:00"
            val bseIndicesKey = new BSEIndicesKey(index, dtf.parseLocalDateTime(dateStr).toDate)
            val bseIndices = new BSEIndices(bseIndicesKey,
              if (csvRecord.get("Open").length == 0)  0 else csvRecord.get("Open").toDouble,
              if (csvRecord.get("High").length == 0)  0 else csvRecord.get("High").toDouble,
              if (csvRecord.get("Low").length == 0)  0 else csvRecord.get("Low").toDouble,
              if (csvRecord.get("Close").length == 0)  0 else csvRecord.get("Close").toDouble)
            bseIndices
          }
        } flatMap {
            case ar => ar.toList
          }

    bseIndicesList map {
      case x => logger.info(x)
    }
  }
}