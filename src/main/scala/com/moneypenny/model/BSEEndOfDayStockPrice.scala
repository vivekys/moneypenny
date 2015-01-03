package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/download/BhavCopy/Equity/EQ281114_CSV.ZIP
case class BSEEndOfDayStockPrice(date : Date, scCode : Long, scName : String, openPrice	: Double, highPrice	: Double,
                                 lowPrice : Double,	closePrice : Double, 	wap : Double, 	numShares	: Long, numTrades	: Long,
                                 totalTurnover : Double, 	deliverableQuantity	: Double, percentDeliQtyToTradedQty	: Double,
                                 spreadHighLow : Double,	spreadCloseOpen : Double) {

}
