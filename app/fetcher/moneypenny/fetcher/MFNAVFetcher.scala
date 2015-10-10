package fetcher.moneypenny.fetcher

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.moneypenny.util.RetryFunExecutor
import com.ning.http.client.AsyncHandler.STATE
import com.ning.http.client.{Response, HttpResponseBodyPart, AsyncCompletionHandler, AsyncHttpClient}
import com.ning.http.client.AsyncHttpClientConfig.Builder
import org.slf4j.LoggerFactory

/**
 * Created by vivek on 10/10/15.
 */
class MFNAVFetcher {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val config = new Builder().setRequestTimeout(600000).setConnectTimeout(600000).build()
  val asyncHttpClient = new AsyncHttpClient(config)

  val url = "http://portal.amfiindia.com/spages/NAV0.txt"

  def getNAV = {
    val sdf = new SimpleDateFormat("MMM-dd-yyyy")
    val date = sdf.format(Calendar.getInstance.getTime)
    logger.info(s"Fetching NAV for $date")

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
      case ex : Exception => logger.error(s"Error while running NAV for $date", ex)
        throw ex
    }
  }
}

object MFNAVFetcher {
  def main (args: Array[String]): Unit = {
    val mfNAVFetcher = new MFNAVFetcher
    val nav = mfNAVFetcher.getNAV
    println(nav)
  }
}
