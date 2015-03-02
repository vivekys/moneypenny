package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html.{HtmlAnchor, HtmlImageInput, HtmlPage, HtmlSelect}
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, Page, WebClient}
import org.apache.log4j.Logger

/**
 * Created by vives on 12/31/14.
 */
class BSEListOfScripsFetcher {
  val webClient = new WebClient(BrowserVersion.CHROME)
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetch = {
    logger.info("Fetching list of Scrips")
    val page = webClient.getPage("http://www.bseindia.com/corporates/List_Scrips.aspx?expandable=1").asInstanceOf[HtmlPage]
    val htmlSelect = page.getElementByName("ctl00$ContentPlaceHolder1$ddSegment").asInstanceOf[HtmlSelect]
    val opt = htmlSelect.getOptionByValue("Equity")
    htmlSelect.setSelectedAttribute(opt, true)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val downloadPage = newPage.getElementById("ctl00_ContentPlaceHolder1_lnkDownload").
      asInstanceOf[HtmlAnchor].click.asInstanceOf[Page]
    val content = downloadPage.getWebResponse.getContentAsString
    content
  }
}

object BSEListOfScripsFetcher {
  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseListOfScripsFetcher = new BSEListOfScripsFetcher
    val bseListOfScrips = bseListOfScripsFetcher.fetch
    println(bseListOfScrips)
  }
}