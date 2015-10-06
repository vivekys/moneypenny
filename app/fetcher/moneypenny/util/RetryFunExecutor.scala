package com.moneypenny.util

import scala.util

/**
 * Created by vives on 3/8/15.
 */

object RetryFunExecutor {
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => Thread.sleep(30000)
                         retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }

}
