package fetcher.moneypenny.fetcher

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit
import java.util.{Locale, Calendar}

import com.moneypenny.util.RetryFunExecutor
import com.ning.http.client.AsyncHandler.STATE
import com.ning.http.client.AsyncHttpClientConfig.Builder
import com.ning.http.client._
import org.apache.http.client.utils.URIBuilder
import org.slf4j.LoggerFactory

/**
 * Created by vivek on 10/10/15.
 */
class MFHistoricNAVFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val config = new Builder().setRequestTimeout(600000).setConnectTimeout(600000).build()

  val asyncHttpClient = new AsyncHttpClient(config)

  val baseURL = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
  val frmdt = "01-Jan-1990"
  val date = Calendar.getInstance
  val day = "%02d".format(date.get(Calendar.DAY_OF_MONTH))
  val month = date.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.getDefault)
  val year = "%02d".format(date.get(Calendar.YEAR))
  val todt = day + "-" + month + "-" + year

  def getNAV(mf : String, tp : String) = {
    val urlBuilder = new URIBuilder(baseURL)
    val url = urlBuilder.addParameter("mf", mf).addParameter("tp", tp).
      addParameter("frmdt", frmdt).addParameter("todt", todt).build().toURL.toString

    logger.info(s"Fetching Historic NAV for $url")
    try {
      RetryFunExecutor.retry(3) {
        asyncHttpClient.prepareGet(url).execute(new AsyncCompletionHandler[Option[String]](){
          val bytes = new ByteArrayOutputStream

          override def onBodyPartReceived(bodyPart : HttpResponseBodyPart) : STATE = {
            bytes.write(bodyPart.getBodyPartBytes())
            STATE.CONTINUE
          }

          override def onCompleted(response: Response): Option[String] = {
            if (response.getStatusCode >= 400)
              None
            else
              Some(bytes.toString("UTF-8"))
          }
        }).get(15, TimeUnit.MINUTES)
      }
    } catch {
      case ex : Exception => logger.error(s"Error while running getHistoricNAV for $mf", ex)
        throw ex
    }
  }
}

object MFHistoricNAVFetcher {
  def main (args: Array[String]): Unit = {
    val mfNAVFetcher = new MFHistoricNAVFetcher
    val nav = mfNAVFetcher.getNAV("21", "1")
    println(nav)
  }
}
