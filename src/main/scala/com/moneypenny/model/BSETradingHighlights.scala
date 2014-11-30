package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/markets/equity/EQReports/Eq.aspx?expandable=7
case class BSETradingHighlights(date : Date, group : String, scripsTraded : Long, numTrades : Long,
                                tradedQty : Long, turnover : Float) {

}
