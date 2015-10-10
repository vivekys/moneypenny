package fetcher.moneypenny.fetcher

import java.util.concurrent.TimeUnit
import java.util.Calendar

import com.moneypenny.util.RetryFunExecutor
import com.ning.http.client.{Response, AsyncCompletionHandler, AsyncHttpClient}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

/**
 * Created by vivek on 04/10/15.
 */
class YahooAdjustedClosePriceFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val asyncHttpClient = new AsyncHttpClient

  val template = "http://real-chart.finance.yahoo.com/table.csv?s="
  val suffix = "&a=00&b=01&c=1991&d=MONTH&e=DAY&f=YEAR&g=d&ignore=.csv"

  def fetchFor (symbol : String, scripName : String, exchange : String) = {
    val date = Calendar.getInstance

    val day = "%02d".format(date.get(Calendar.DAY_OF_MONTH))
    val month = "%02d".format(date.get(Calendar.MONTH))
    val year = "%02d".format(date.get(Calendar.YEAR))

    val url = template + symbol + suffix replace("DAY", day) replace("MONTH", month) replace("YEAR", year)

    logger.info(s"Fetching adjusted closing price for $url")

    try {
      RetryFunExecutor.retry(3) {
        asyncHttpClient.prepareGet(url).execute(new AsyncCompletionHandler[Option[String]](){
          override def onCompleted(response: Response): Option[String] = {
            if (response.getStatusCode >= 400)
              None
            else
              Some(response.getResponseBody)
          }
        }).get(30, TimeUnit.SECONDS)
      }
    } catch {
      case ex : Exception => logger.error(s"Error while running YahooAdjustedClosePriceFetcher for $url", ex)
        throw ex
    }
  }
}
