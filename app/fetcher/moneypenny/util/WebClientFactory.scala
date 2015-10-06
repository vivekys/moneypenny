package fetcher.moneypenny.util

import com.gargoylesoftware.htmlunit.{NicelyResynchronizingAjaxController, BrowserVersion, WebClient}

/**
 * Created by vivek on 05/09/15.
 */
object WebClientFactory {
  def getWebClient = {
    val webClient = new WebClient(BrowserVersion.INTERNET_EXPLORER_11)
    webClient.getOptions().setThrowExceptionOnScriptError(false)
    webClient.setAjaxController(new NicelyResynchronizingAjaxController())
    webClient
  }
}
