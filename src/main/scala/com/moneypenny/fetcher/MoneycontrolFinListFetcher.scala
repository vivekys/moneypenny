package com.moneypenny.fetcher

import java.util

import com.gargoylesoftware.htmlunit.html.{HtmlAnchor, DomNodeList, HtmlUnorderedList, HtmlPage}
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

/**
 * Created by vives on 1/2/15.
 */
object MoneycontrolFinListFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  private val config = ConfigFactory.load
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.getOptions.setJavaScriptEnabled(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def isActive (pageContent : String) = {
    ! (pageContent.contains("is not traded on BSE in the last 30 days") ||
        pageContent.contains("is not traded on NSE in the last 30 days"))
  }

  def fetchFinURLs (url : String) = {
    logger.info("Fetching Moneycontrol stock Financial anchors")
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    if (isActive(page.asText())) {
      val xpath = config.getString("com.moneypenny.xpath.MoneycontrolFinListFetcher")
      val unorderedList = page.getByXPath(xpath).get(0).asInstanceOf[HtmlUnorderedList]
      val anchorList = unorderedList.getElementsByTagName("a").asInstanceOf[DomNodeList[HtmlAnchor]]
      for (anchor <- anchorList) {
        if (!anchor.getFirstChild.getNodeValue.contains("Financial Graphs")) {
          logger.debug("Fetching Moneycontrol stock Financial anchors - " +
            page.getFullyQualifiedUrl(anchor.getHrefAttribute).toString)
          returnMap.put(anchor.getFirstChild.getNodeValue, page.getFullyQualifiedUrl(anchor.getHrefAttribute).toString)
        }
      }

    }
    returnMap
  }

  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)
//    println(fetchFinURLs ("http://www.moneycontrol.com/india/stockpricequote/financeleasinghirepurchase/akscredits/AKS"))
    println(fetchFinURLs ("http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01"))
  }

}
