package com.moneypenny.manager

import com.moneypenny.fetcher.BSEIndicesFetcher
import org.quartz.{JobExecutionContext, Job}

/**
 * Created by vives on 2/17/15.
 */
class BSEIndicesManager extends Job {

  val bseIndicesFetcher = new BSEIndicesFetcher

  override def execute(jobExecutionContext: JobExecutionContext): Unit = ???
}
