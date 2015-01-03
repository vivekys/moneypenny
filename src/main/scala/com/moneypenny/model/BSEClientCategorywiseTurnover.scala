package com.moneypenny.model

/**
 * Created by vives on 1/1/15.
 */
//http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&flag=1
case class BSEClientCategorywiseTurnover (tradeDate : String,	clientsBuy : Double,	clientsSales : Double, clientsNet	: Double,
NRIBuy : Double,	NRISales : Double,	NRINet : Double,	proprietaryBuy : Double, proprietarySales : Double,
ProprietaryNet : Double,	IFIsBuy : Double,	IFIsSales : Double, IFIsNet : Double, 	banksBuy : Double,
banksSales : Double,	banksNet : Double, insuranceBuy : Double, insuranceSales : Double,	insuranceNet : Double,
DIIBuy : Double, DIISales : Double,	DIINet : Double) {

}


