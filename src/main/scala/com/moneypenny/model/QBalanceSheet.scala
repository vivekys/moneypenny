package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */
//http://www.bseindia.com/corporates/results.aspx?Code=500570&Company=TATA%20MOTORS%20LTD.&qtr=83.00&RType=
case class QBalanceSheet(scriptCode : Long, companyName : String, date : Date,
                         attributes : Map[String, Float]) {

}
