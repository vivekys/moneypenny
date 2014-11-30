package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/markets/Equity/EQReports/Historical_EquitySegment.aspx?expandable=7
case class BSEEquityMarketSummary(date : Date, numCompaniesTraded : Long, numTrades : Long,
                                  numShares : Long, netTurnOver : Float) {

}
