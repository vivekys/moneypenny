package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.cdslindia.com/publications/ArchiveDataAfter2009.aspx?dateSelected=28-NOV-2014
case class FIIDailyDerivativeActivity(date : Date, investmentType : String, //INDEX_FUTURES, INDEX_OPTIONS, STOCK_FUTURES, STOCK_OPTIONS
                                      buyNumContracts : Long, buyAmountContracts : Float,
                                      sellNumContracts : Long, sellAmountContracts : Float,
                                      OINumContracts : Long, OIAmountContracts : Float) {

}
