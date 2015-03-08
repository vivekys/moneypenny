package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html.{DomNodeList, HtmlAnchor, HtmlPage, HtmlUnorderedList}
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
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

  def isActiveOnBSE (pageContent : String) = {
    ! ((pageContent.contains("is not traded on BSE in the last 30 days")) ||
      pageContent.contains("is not listed on BSE"))
  }

  def isActiveOnNSE (pageContent : String) = {
    ! ((pageContent.contains("is not traded on NSE in the last 30 days")) ||
      pageContent.contains("is not listed on NSE"))
  }

  def fetchGetMetaData (name : String, url : String) = {
    logger.info("Fetching Moneycontrol stock Metadata")
    val returnMap = scala.collection.mutable.Map.empty[String, Map[String, String]]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]

    val meta = page.asText().split("\n").filter(line => {
                  line.contains("BSE: ") || line.contains("NSE: ") || line.contains("ISIN: ") || line.contains("SECTOR: ")
               }).map(_.split('|')).flatMap{case arrStr : Array[String] => arrStr}.map(ele => {
                  val kv = ele.split(":")
                  (kv(0), kv(1).trim)
               }).toMap
    returnMap.put(name, meta)
    returnMap
  }

  def fetchFinURLs (url : String) = {
    logger.info("Fetching Moneycontrol stock Financial anchors")
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    val page = webClient.getPage(url).asInstanceOf[HtmlPage]
    if (isActiveOnBSE(page.asText()) || isActiveOnNSE(page.asText())) {
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
//    println(fetchFinURLs ("http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01"))
    fetchGetMetaData("Tata Motors", "http://www.moneycontrol.com/india/stockpricequote/autolcvshcvs/tatamotors/TM03")
  }

}
