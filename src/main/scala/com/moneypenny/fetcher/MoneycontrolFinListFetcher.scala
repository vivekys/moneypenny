package com.moneypenny.fetcher

import java.util

import com.gargoylesoftware.htmlunit.html.{HtmlAnchor, DomNodeList, HtmlUnorderedList, HtmlPage}
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

/**
 * Created by vives on 1/2/15.
 */
object MoneycontrolFinListFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.getOptions.setJavaScriptEnabled(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def isActive (pageContent : String) = {
    ! (pageContent.contains("is not traded on BSE in the last 30 days") ||
        pageContent.contains("is not traded on NSE in the last 30 days"))
  }

  def fetchFinURLs (url : String) = {
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    if (isActive(page.asText())) {
      val unorderedList = page.getByXPath("//*[@id=\"slider\"]/dd[3]/ul").get(0).asInstanceOf[HtmlUnorderedList]
      val anchorList = unorderedList.getElementsByTagName("a").asInstanceOf[DomNodeList[HtmlAnchor]]
      for (anchor <- anchorList) {
        if (anchor.getFirstChild.getNodeValue != "Financial Graphs")
          returnMap.put(anchor.getFirstChild.getNodeValue, page.getFullyQualifiedUrl(anchor.getHrefAttribute).toString)
      }

    }
    returnMap
  }

  def main (args: Array[String]) {
    println(fetchFinURLs ("http://www.moneycontrol.com/india/stockpricequote/financeleasinghirepurchase/akscredits/AKS"))
    println(fetchFinURLs ("http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01"))
  }

}
