package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html.{HtmlAnchor, HtmlImageInput, HtmlSelect, HtmlPage}
import com.gargoylesoftware.htmlunit.{Page, NicelyResynchronizingAjaxController, BrowserVersion, WebClient}

/**
 * Created by vives on 12/31/14.
 */
class BSEListOfScripsFetcher {
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetch = {
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
    val bseListOfScripsFetcher = new BSEListOfScripsFetcher
    val bseListOfScrips = bseListOfScripsFetcher.fetch
    println(bseListOfScrips)
  }
}