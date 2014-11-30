package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.cdslindia.com/publications/ArchiveDataBefore2009.aspx?dateSelected=30-NOV-2001
//http://www.cdslindia.com/publications/ArchiveDataAfter2009.aspx?dateSelected=28-NOV-2014
case class FIIDailyCashActivity(date : Date, investmentType : String, //Equity or Debt
                                FIIBuy : Float, FIISell : Float, FIINet : Float) {

}
