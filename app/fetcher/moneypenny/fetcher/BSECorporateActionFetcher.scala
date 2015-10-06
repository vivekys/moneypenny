package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, Page, WebClient}
import com.moneypenny.db.MongoContext
import com.moneypenny.model.{BSECorporateAction, BSECorporateActionKey, BSEListOfScrips, BSEListOfScripsDAO}
import com.moneypenny.util.RetryFunExecutor
import fetcher.moneypenny.util.WebClientFactory
import org.apache.commons.csv.{CSVRecord, CSVFormat, CSVParser}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.parallel.immutable.ParSeq

/**
 * Created by vives on 2/7/15.
 *
 * Always fetch from the beginning of time till current date
 */
class BSECorporateActionFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def fetchListOfScrips = {
    val context = new MongoContext
    context.connect()

    val dao = new BSEListOfScripsDAO(context.bseListOfScripsCollection)
    dao.findAll
  }

  def fetchAll (startDate : String, endDate : String) = {
    logger.info(s"Fetching CA from $startDate till $endDate")

    val webClient = WebClientFactory.getWebClient

    val page = webClient.getPage("http://www.bseindia.com/corporates/corporate_act.aspx?expandable=0").asInstanceOf[HtmlPage]

    val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtDate").asInstanceOf[HtmlInput]
    fromDate.setValueAttribute(startDate)

    val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtTodate").asInstanceOf[HtmlInput]
    toDate.setValueAttribute(endDate)

    val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
    val newPage = submitBtn.click().asInstanceOf[HtmlPage]
    val content = if (!newPage.asText.contains("No Corporate Actions During Selected Period")) {
      newPage.getElementById("ctl00_ContentPlaceHolder1_lnkDownload1").
        asInstanceOf[HtmlAnchor].click.asInstanceOf[Page].getWebResponse.getContentAsString
    } else
      ""

    val dtf = DateTimeFormat.forPattern("dd MMM yyyy")

    val records = try {
      CSVParser.parse(content, CSVFormat.EXCEL.withHeader().withIgnoreSurroundingSpaces(true)).getRecords.par
    } catch {
      case ex : Exception => logger.info("Exception while parsing BSECorporateAction records with data as ", ex)
        ParSeq.empty[CSVRecord]
    }

    records flatMap {
      case csvRecord => try {
        val scripCode = csvRecord.get("Security Code").toLong
        val scripName = csvRecord.get("Security Name")
        Some(BSECorporateAction(BSECorporateActionKey(scripCode, scripName,
          if (csvRecord.get("Ex Date").length == 0 || csvRecord.get("Ex Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Ex Date")).toDate),
          if (csvRecord.get("Purpose").length == 0) None else Some(csvRecord.get("Purpose")),
          if (csvRecord.get("Record Date").length == 0 || csvRecord.get("Record Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Record Date")).toDate),
          if (csvRecord.get("BC Start Date").length == 0 || csvRecord.get("BC Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC Start Date")).toDate),
          if (csvRecord.get("BC End Date").length == 0 || csvRecord.get("BC End Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC End Date")).toDate),
          if (csvRecord.get("ND Start Date").length == 0 || csvRecord.get("ND Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND Start Date")).toDate),
          if (csvRecord.get("ND End Date").length == 0 || csvRecord.get("ND End Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND End Date")).toDate),
          if (csvRecord.get("Actual Payment Date").length == 0 || csvRecord.get("Actual Payment Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Actual Payment Date")).toDate))
        ))
      } catch {
        case ex : Exception => logger.info("Exception while parsing BSECorporateAction records with csvRecord as " + csvRecord, ex)
          None
      }
    }
  }

  def fetchCAForId (startDate : String, endDate : String, id : String) = {
    logger.info(s"Fetching CA for $id from $startDate till $endDate")

    val webClient = new WebClient(BrowserVersion.CHROME)
    webClient.getOptions().setThrowExceptionOnScriptError(false)
    webClient.setAjaxController(new NicelyResynchronizingAjaxController())

    val page = webClient.getPage("http://www.bseindia.com/corporates/corporate_act.aspx?expandable=0").asInstanceOf[HtmlPage]

    val searchInput = page.getElementByName("ctl00$ContentPlaceHolder1$GetQuote1_smartSearch").asInstanceOf[HtmlTextInput]
    searchInput.`type`(id)
    val list = page.getElementById("listEQ").asInstanceOf[HtmlUnorderedList]
    if (list != null && list.getElementsByTagName("a").length != 0) {
      val element = list.getElementsByTagName("a").get(0).asInstanceOf[HtmlAnchor]
      element.click()

      val fromDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtDate").asInstanceOf[HtmlInput]
      fromDate.setValueAttribute(startDate)

      val toDate = page.getElementByName("ctl00$ContentPlaceHolder1$txtTodate").asInstanceOf[HtmlInput]
      toDate.setValueAttribute(endDate)

      val submitBtn = page.getElementByName("ctl00$ContentPlaceHolder1$btnSubmit").asInstanceOf[HtmlImageInput]
      val newPage = submitBtn.click().asInstanceOf[HtmlPage]
      val content = if (!newPage.asText.contains("No Corporate Actions During Selected Period")) {
        newPage.getElementById("ctl00_ContentPlaceHolder1_lnkDownload1").
          asInstanceOf[HtmlAnchor].click.asInstanceOf[Page].getWebResponse.getContentAsString
      } else
        ""
      content
    } else
      ""
  }

  def fetch (startDate : String, endDate : String) = {
    val returnMap = scala.collection.mutable.Map.empty[(Long, String, String), String]

    val bseListOfScrips = fetchListOfScrips
    logger.info("Fetching CA for " + bseListOfScrips.length + " list of companies")
    bseListOfScrips.filter((bseScrip : BSEListOfScrips) => bseScrip.status.get != "Delisted" &&
      bseScrip.status.get != "N").par map {
      case bseScrip => {
        val scripCode = bseScrip._id.scripCode
        val scripId = bseScrip.scripId.get
        val scripName = bseScrip.scripName.get
        logger.info(s"$scripCode, $scripId, $scripName")
        try {
          val data = RetryFunExecutor.retry(3)(fetchCAForId(startDate, endDate, scripId))
          val (key, value) = ((scripCode, scripId, scripName), data)
          returnMap.put(key, value)
        } catch {
          case ex : Exception =>
            logger.error(s"Error while fetching CA for $scripCode, $scripId, $scripName", ex)
        }
      }
    }
    returnMap
  }
}


object BSECorporateActionFetcher {
  def main (args: Array[String]) {

    java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.SEVERE)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
    val bseCorporateAction = new BSECorporateActionFetcher
    val data = bseCorporateAction.fetchAll("01/01/1990", "28/01/2015")

    println(data)
  }
}
