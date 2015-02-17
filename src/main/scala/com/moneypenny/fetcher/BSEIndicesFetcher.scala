package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.log4j.Logger

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
    returnMap
  }
}

object BSEIndicesFetcher {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseIndicesFetcher = new BSEIndicesFetcher
    val indices = bseIndicesFetcher.fetch("01/01/2015", "01/02/2015")
    println(indices)
  }
}