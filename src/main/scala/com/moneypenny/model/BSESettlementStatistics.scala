package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/markets/equity/EQReports/settlement_stats.aspx?expandable=7
case class BSESettlementStatistics(date : Date, numTrades : Long, QTYOfSharesTraded : Float, QTYOfSharesDelivered : Float,
                                    perSharesDelToTraded : Float, valueOfSharesTraded : Float,
                                    valueOfSharesDelivered : Float, perValueDelToTraded : Float) {

}
