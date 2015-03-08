package com.moneypenny.fetcher

import com.gargoylesoftware.htmlunit.html._
import com.gargoylesoftware.htmlunit.{BrowserVersion, NicelyResynchronizingAjaxController, Page, WebClient}
import com.moneypenny.db.MongoContext
import com.moneypenny.model.{BSEListOfScrips, BSEListOfScripsDAO, BSECorporateAction, BSECorporateActionKey}
import com.moneypenny.util.RetryFunExecutor
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConversions._

/**
 * Created by vives on 2/7/15.
 */
class BSECorporateActionFetcher {
  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def fetchListOfScrips = {
    val context = new MongoContext
    context.connect()

    val dao = new BSEListOfScripsDAO(context.bseListOfScripsCollection)
    dao.findAll
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
  }

  def fetch (startDate : String, endDate : String) = {
    val returnMap = scala.collection.mutable.Map.empty[(Long, String, String), String]

    val bseListOfScrips = fetchListOfScrips

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
    org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("org.apache.commons.httpclient").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org.apache.http").setLevel(org.apache.log4j.Level.OFF)

    val bseCorporateAction = new BSECorporateActionFetcher
    val data = bseCorporateAction.fetch("01/01/1990", "28/01/2015")

    val dtf = DateTimeFormat.forPattern("dd MMM yyyy")
    val bseCorporateActionList = data map {
      case (key, value) => {
        val (scripCode, scripId, scripName) = key
        CSVParser.parse(value, CSVFormat.EXCEL.withHeader()).getRecords map {
          case csvRecord => BSECorporateAction(BSECorporateActionKey(scripCode, scripId, scripName,
            if (csvRecord.get("Ex Date").length == 0 || csvRecord.get("Ex Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Ex Date")).toDate),
            if (csvRecord.get("Purpose").length == 0) None else Some(csvRecord.get("Purpose")),
            if (csvRecord.get("Record Date").length == 0 || csvRecord.get("Record Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Record Date")).toDate),
            if (csvRecord.get("BC Start Date").length == 0 || csvRecord.get("BC Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC Start Date")).toDate),
            if (csvRecord.get("BC End Date	").length == 0 || csvRecord.get("BC End Date	").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("BC End Date	")).toDate),
            if (csvRecord.get("ND Start Date").length == 0 || csvRecord.get("ND Start Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND Start Date")).toDate),
            if (csvRecord.get("ND End Date").length == 0 || csvRecord.get("ND End Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("ND End Date")).toDate),
            if (csvRecord.get("Actual Payment Date").length == 0 || csvRecord.get("Actual Payment Date").length == 1) None else Some(dtf.parseLocalDateTime(csvRecord.get("Actual Payment Date")).toDate)))
        }
      }
    } flatMap {
      case ar => ar.toList
    }
    println(bseCorporateActionList)
  }
}
