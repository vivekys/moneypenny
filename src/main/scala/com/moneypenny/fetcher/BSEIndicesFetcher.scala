package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{WebClient, BrowserVersion}
import scala.collection.JavaConversions._


/**
 * Created by vives on 12/29/14.
 */
class BSEIndicesFetcher {
  val webClient = new WebClient(BrowserVersion.CHROME)

  def fetchOptions = {
    val page = webClient.getPage("http://www.bseindia.com/indices/IndexArchiveData.aspx?expandable=1").asInstanceOf[HtmlPage]
    val htmlTable = page.getHtmlElementById("DMY").asInstanceOf[HtmlTable]
    val htmlSelect = page.getElementByName("ctl00$ContentPlaceHolder1$ddlIndex").asInstanceOf[HtmlSelect]
    val len = htmlSelect.getOptions.length
    for (i <- 1 until len) yield htmlSelect.getOptions.get(i)


  }
  def fetchDataForOption (startDate : String, endDate : String, option : String) = {
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
    val bseIndicesFetcher = new BSEIndicesFetcher
    val indices = bseIndicesFetcher.fetch("01/01/1990", "31/12/2014")
    println(indices)
  }
}