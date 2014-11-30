package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */

//http://www.nseindia.com/content/historical/EQUITIES/2014/NOV/cm28NOV2014bhav.csv.zip

case class NSEEndOfDayStockPrice(symbol : String, series : String, open : Float,
                                  high : Float, low : Float, close : Float, last : Float,
                                  prevClose : Float, totalTradedQTY : Long, totalTradedVal : Float, timestamp : Date,
                                  totalTrades : Long, ISIN : String) {

}
