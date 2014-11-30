package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&flag=1
case class InvestorCategorywiseTurnover(date : Date, clientsBuy : Float, clientsSell : Float, clientsNet : Float,
                                         NRIBuy : Float, NRISell : Float, NRINet : Float,
                                         ProprietaryBuy : Float, ProprietarySell : Float, ProprietaryNet : Float,
                                         DIIBuy : Float, DIISell : Float, DIINet : Float,
                                         FIIBuy : Float, FIISell : Float, FIINet : Float) {

}
