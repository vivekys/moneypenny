package com.moneypenny.fetcher

import java.util.concurrent.Executors

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, WebClient}
import com.moneypenny.model.{MoneycontrolListOfScrips, MoneycontrolListOfScripsKey}
import com.moneypenny.util.RetryFunExecutor
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/**
 * Created by vives on 1/2/15.
 */
object MoneycontrolStockListFetcher {
  val baseURL = "http://www.moneycontrol.com/india/stockmarket/pricechartquote/"
  val postfixURL = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
                    "T", "U", "V", "W", "X", "Y", "Z", "others")

  val logger = Logger.getLogger(this.getClass.getSimpleName)
  private val config = ConfigFactory.load

  def fetchAnchors () = {
    val webClient = new WebClient(BrowserVersion.CHROME)
    webClient.getOptions().setThrowExceptionOnScriptError(false)
    webClient.getOptions.setJavaScriptEnabled(false)
    webClient.setAjaxController(new NicelyResynchronizingAjaxController())

    logger.info("Fetching Moneycontrol stock anchors")
    val returnMap = scala.collection.mutable.Map.empty[String, String]
    for (postfix <- postfixURL) {
      val page = webClient.getPage(baseURL + postfix).asInstanceOf[HtmlPage]
      val xpath = config.getString("com.moneypenny.xpath.MoneycontrolStockListFetcher")
      val htmlTable = page.getByXPath(xpath).get(0).asInstanceOf[HtmlTable]

      val anchorList = htmlTable.getElementsByTagName("a").asInstanceOf[DomNodeList[HtmlAnchor]]
      for (anchor <- anchorList) {
        if (!anchor.getHrefAttribute.isEmpty) {
          logger.info("Fetching Moneycontrol stock anchors - " + anchor.getHrefAttribute + " for " + anchor.getTextContent)
          returnMap.put(anchor.getTextContent, anchor.getHrefAttribute)
        }
      }
    }
    logger.info("Fetched " + returnMap.size + " Anchors from Moneycontrol")
    returnMap
  }

  def fetchGetMetaData (name : String, url : String) : scala.collection.mutable.Map[String, Map[String, Option[String]]] = {
    val webClient = new WebClient(BrowserVersion.CHROME)
    webClient.getOptions().setThrowExceptionOnScriptError(false)
    webClient.getOptions.setJavaScriptEnabled(false)
    webClient.setAjaxController(new NicelyResynchronizingAjaxController())

    logger.info(s"Fetching Moneycontrol stock Metadata for $name")
    val returnMap = scala.collection.mutable.Map.empty[String, Map[String, Option[String]]]
    try {
      val page = webClient.getPage(url).asInstanceOf[HtmlPage]

      if (MoneycontrolFinListFetcher.isActiveOnBSE(page.asText()) ||
        MoneycontrolFinListFetcher.isActiveOnNSE(page.asText())) {
        val meta = page.asText().split("\n").filter(line => {
          line.contains("BSE: ") || line.contains("NSE: ") || line.contains("ISIN: ") || line.contains("SECTOR: ")
        }).map(_.split('|')).map {
          case arr => arr map {
            case ele =>
              val kv = ele.split(":")
              kv match {
                case Array(key, value) => (key.trim, if (value.trim.length > 1) Some(value.trim) else None)
                case Array(key) => (key.trim, None)
              }
          }
        } flatMap {
          case ar => ar.toMap
        }
        val updatedMeta =  meta.toMap + ("url" -> Some(url))
        returnMap.put(name, updatedMeta)
      }
    } catch {
      case ex : Exception => logger.error(s"Error while fetching meta data for $name", ex)
    }
    returnMap
  }

  def fetch = {
    fetchAnchors.par map {
      case (name, url) => RetryFunExecutor.retry(3)(fetchGetMetaData(name, url))
    }
  }

  def main (args: Array[String]) {
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)
    val list = fetch
    println(list)
  }

}

