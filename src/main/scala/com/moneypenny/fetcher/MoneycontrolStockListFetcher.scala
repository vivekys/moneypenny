package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
/**
 * Created by vives on 1/2/15.
 */
object MoneycontrolStockListFetcher {
  val baseURL = "http://www.moneycontrol.com/india/stockmarket/pricechartquote/"
  val postfixURL = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
                    "T", "U", "V", "W", "X", "Y", "Z", "others")

  val logger = Logger.getLogger(this.getClass.getSimpleName)
  private val config = ConfigFactory.load

  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.getOptions.setJavaScriptEnabled(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchAnchors () = {
    logger.info("Fetching Moneycontrol stock anchors")
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    for (postfix <- postfixURL) {
      val page = webClient.getPage(baseURL + postfix).asInstanceOf[HtmlPage]
      val xpath = config.getString("com.moneypenny.xpath.MoneycontrolStockListFetcher")
      val htmlTable = page.getByXPath(xpath).get(0).asInstanceOf[HtmlTable]

      val anchorList = htmlTable.getElementsByTagName("a").asInstanceOf[DomNodeList[HtmlAnchor]]
      for (anchor <- anchorList) {
        if (!anchor.getHrefAttribute.isEmpty) {
          logger.debug("Fetching Moneycontrol stock anchors - " + anchor.getHrefAttribute + " for " + anchor.getTextContent)
          returnMap.put(anchor.getTextContent, anchor.getHrefAttribute)
        }
      }
    }
    returnMap
  }

  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)
    val stockList = fetchAnchors()
    println(stockList.size)
    println(stockList)
  }

}

