package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html.{DomNodeList, HtmlAnchor, HtmlTable, HtmlPage}
import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
 * Created by vives on 1/2/15.
 */
object MoneycontrolStockListFetcher {
  val baseURL = "http://www.moneycontrol.com/india/stockmarket/pricechartquote/"
  val postfixURL = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
                    "T", "U", "V", "W", "X", "Y", "Z", "others")

  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val webClient = new WebClient(BrowserVersion.CHROME)
  webClient.getOptions().setThrowExceptionOnScriptError(false)
  webClient.getOptions.setJavaScriptEnabled(false)
  webClient.setAjaxController(new NicelyResynchronizingAjaxController())

  def fetchAcnhors () = {
    val returnStockList = scala.collection.mutable.ArrayBuffer.empty[String]
    for (postfix <- postfixURL) {
      val page = webClient.getPage(baseURL + postfix).asInstanceOf[HtmlPage]
      val htmlTable = page.getByXPath("/html/body/center[2]/div/div[1]/div[6]/div[2]/table").get(0).asInstanceOf[HtmlTable]
      val anchorList = htmlTable.getElementsByTagName("a").asInstanceOf[DomNodeList[HtmlAnchor]]
      for (anchor <- anchorList) {
        returnStockList += anchor.getHrefAttribute
      }
    }
    returnStockList
  }

  def main (args: Array[String]) {
    val stockList = fetchAcnhors()
    println(stockList.length)
    println(stockList)
  }

}

