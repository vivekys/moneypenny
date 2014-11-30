package com.moneypenny.model

import java.util.Date

/**
 * Created by vives on 11/30/14.
 */

//http://www.bseindia.com/BSEDATA/gross/2014/SCBSEALL2811.zip
case class BSEGrossDeliverables(date : Date, scripCode : String, deliveryQty : Long, deliveryVal : Long,
                                 daysVolume : Long, daysTurnover : Long, deliveryPer : Float) {

}
