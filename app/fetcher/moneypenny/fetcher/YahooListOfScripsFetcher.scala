package fetcher.moneypenny.fetcher

import java.net.URI
import java.util.concurrent.TimeUnit

import com.ning.http.client.{Response, AsyncCompletionHandler, AsyncHttpClient}
import org.apache.http.client.utils.URLEncodedUtils
import org.jsoup.Jsoup
import org.slf4j.LoggerFactory
import scalaz.Scalaz._

import scala.collection.JavaConversions._

/**
 * Created by vivek on 04/10/15.
 */
class YahooListOfScripsFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val asyncHttpClient = new AsyncHttpClient
  val template = "https://in.finance.yahoo.com/lookup/stocks;?s="
  val suffix = "&t=S&m=IN&r="
  val nextSuffix = "&t=S&m=IN&r=&b="
  val baseUrl = "https://in.finance.yahoo.com"

  def getNextUrlTemplate (url : String) = {
    logger.info("Getting NextUrlTemplate " + url)
    asyncHttpClient.prepareGet(url).execute(new AsyncCompletionHandler[(Option[String], Option[String])](){
      override def onCompleted(response: Response): (Option[String], Option[String]) = {
        try {
          val document = response.getResponseBody
          val dom = Jsoup.parse(document)
          val pagination = dom.getElementById("pagination")
          val nextEle = pagination.getElementsContainingText("Next")
          val lastEle = pagination.getElementsContainingText("Last")
          (Some(nextEle.attr("href")), Some(lastEle.attr("href")))
        } catch {
          case ex : Exception => {
            logger.info("Failed to Fetch " + url)
            (None, None)
          }
        }
      }
    }).get(30, TimeUnit.SECONDS)
  }

  def getScrips (url : String) = {
    asyncHttpClient.prepareGet(url).execute(new AsyncCompletionHandler[Map[String, (String, String)]](){

      override def onCompleted(response: Response): Map[String, (String, String)] = {
        val document = response.getResponseBody
        val dom = Jsoup.parse(document).getElementById("yfi_sym_lookup")
        if (dom != null) {
          val table = dom.select("tbody")
          val rows = table.select("tr")
          rows.map(row => {
            try {
              val symbol = row.select("td").get(0)
              val name = row.select("td").get(1)
              val exchange = row.select("td").get(4)
              (Some(symbol.text()), (Some(name.text()), Some(exchange.text())))
            } catch {
              case ex : Exception => {
                logger.info("Failed to extract for URL " + url)
                (None, (None, None))
              }
            }
          }) filter (_._1.isDefined) map {
            case (Some(symbol), (Some(scripName), Some(exchange))) => (symbol, (scripName, exchange))
            case _ => ("", ("", ""))
          } toMap
        }
        else {
          Map.empty[String, (String, String)]
        }
      }
    }).get(30, TimeUnit.SECONDS)
  }

  def fetchFor(letter : String) = {
    val url = template + letter + suffix
    val (next, last) = getNextUrlTemplate(url)
    val list = (next, last) match {
      case (Some(n), Some(l)) => {
        val nextParams = URLEncodedUtils.parse(new URI(n), "UTF-8").map (x => (x.getName -> x.getValue)).toMap
        val lastParams = URLEncodedUtils.parse(new URI(l), "UTF-8").map (x => (x.getName -> x.getValue)).toMap
        val inc = nextParams.get("b").get.toInt
        val lastPage = lastParams.get("b").get.toInt
        0 to lastPage by inc
      }
      case _ => {
        0 to 0
      }
    }

    list.par map (limit => {
      val url = template + letter + nextSuffix + limit
      logger.info("Fetching Scrips from url" + url)
      getScrips(url)
    }) reduceLeft(_ |+| _)

  }

  def fetchAll () = {
    val char = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
      "T", "U", "V", "W", "X", "Y", "Z", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0")

    char.map( letter => {
      fetchFor(letter)
    }) reduceLeft(_ |+| _)
  }
}

object YahooListOfScripsFetcher {
  def main (args: Array[String]): Unit = {
    val yahooFetcher = new YahooListOfScripsFetcher
    val map = yahooFetcher.fetchAll
    println(map)
    println("Size = " + map.size)
  }
}