package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import com.moneypenny.model.{BSEIndices, BSEIndicesKey}
import com.moneypenny.util.RetryFunExecutor
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


/**
 * Created by vives on 12/29/14.
 */
class BSEIndicesFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def fetchOptions = {
    val webClient = new WebClient(BrowserVersion.CHROME)
    webClient.getOptions().setThrowExceptionOnScriptError(false)
    webClient.setAjaxController(new NicelyResynchronizingAjaxController())

    val page = webClient.getPage("http://www.bseindia.com/indices/IndexArchiveData.aspx?expandable=1").asInstanceOf[HtmlPage]
    val htmlTable = page.getHtmlElementById("DMY").asInstanceOf[HtmlTable]
    val htmlSelect = page.getElementByName("ctl00$ContentPlaceHolder1$ddlIndex").asInstanceOf[HtmlSelect]
    val len = htmlSelect.getOptions.length
    for (i <- 1 until len) yield htmlSelect.getOptions.get(i)


  }
  def fetch (startDate : String, endDate : String, option : String) : Map[(String, String), String] = {
    logger.info(s"Fetching $option from $startDate to $endDate")
    val returnMap = scala.collection.mutable.Map.empty[(String, String), String]

    try {
      RetryFunExecutor.retry(3) {
        val webClient = new WebClient(BrowserVersion.CHROME)
        webClient.getOptions().setThrowExceptionOnScriptError(false)
        webClient.setAjaxController(new NicelyResynchronizingAjaxController())

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
        returnMap.put((opt.getValueAttribute, opt.getText), content)
      }
    }  catch {
      case ex : Exception => logger.info(s"Error while fetching Indices from $startDate to $endDate for $option", ex)
    }
    returnMap.toMap
  }

  def fetch (startDate : String, endDate : String) : Map[(String, String), String] = {
    logger.info(s"Fetching Indices from $startDate to $endDate")
    val returnMap = scala.collection.mutable.Map.empty[(String, String), String]
    val options = fetchOptions
    options.par map {
      case opt =>
        try {
          fetch(startDate, endDate, opt.getValueAttribute) map {
            case (key, value) => returnMap.put((key._1, key._2), value)
          }

        } catch {
          case ex : Exception => logger.info(s"Error while fetching Indices from $startDate to $endDate for "
            + opt.getValueAttribute, ex)
        }
    }
    returnMap.toMap
  }
}

object BSEIndicesFetcher {
  def main (args: Array[String]) {
    val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val bseIndicesFetcher = new BSEIndicesFetcher
    val indices = bseIndicesFetcher.fetch("01/01/1990", "22/02/2015")

    val dtf = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")

    val bseIndicesList =  indices map {
        case ((indexId, indexName), data) => CSVParser.parse(data, CSVFormat.EXCEL.withHeader()).getRecords map {
          csvRecord =>
            val dateStr = csvRecord.get("Date") + " 15:45:00"
            val bseIndicesKey = new BSEIndicesKey(indexId, indexName, dtf.parseLocalDateTime(dateStr).toDate)
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
      case x => logger.info(x.toString)
    }
  }
}