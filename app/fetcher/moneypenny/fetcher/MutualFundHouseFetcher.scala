package fetcher.moneypenny.fetcher

import java.util.concurrent.TimeUnit

import com.moneypenny.util.RetryFunExecutor
import com.ning.http.client.{Response, AsyncCompletionHandler, AsyncHttpClient}
import org.jsoup.Jsoup
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Created by vivek on 10/10/15.
 */
class MutualFundHouseFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val asyncHttpClient = new AsyncHttpClient
  val baseUrl = "https://www.amfiindia.com/nav-history-download"

  def getMFHouses = {
    val url = baseUrl
    logger.info("Fetching MFHouses from url" + url)
    try {
      RetryFunExecutor.retry(3) {
        asyncHttpClient.prepareGet(url).execute(new AsyncCompletionHandler[Map[String, String]](){
          override def onCompleted(response: Response): Map[String, String] = {
            val document = response.getResponseBody
            val list = Jsoup.parse(document).getElementById("NavDownMFName")
            val options = list.select("select > option")
            options map {
              option => (option.attr("value"), option.text())
            } toMap
          }
        }).get(30, TimeUnit.SECONDS)
      }
    } catch {
      case ex : Exception => logger.error(s"Error while running getMFHouses for $url", ex)
        throw ex
    }
  }
}


object MutualFundHouseFetcher {
  def main (args: Array[String]): Unit = {
    val mfHouse = new MutualFundHouseFetcher
    val map = mfHouse.getMFHouses
    println(map)
    println("Size = " + map.size)
  }
}
