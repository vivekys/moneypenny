package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/download/BhavCopy/Equity/EQ281114_CSV.ZIP
case class BSEEndOfDayStockPrice(date : Date, scCode : Long, scName : String, scGroup : String, scType : String,
                                  open : Float, high : Float, Low : Float, clost : Float, last : Float,
                                  prevClose : Float, numTrades : Long, numShares : Long, netTurnOver : Float,
                                  TDCLOINDI : String) {

}
