package com.moneypenny.model

/**
 * Created by vives on 11/30/14.
 */

//http://www.bseindia.com/corporates/List_Scrips.aspx?expandable=1
case class BSEListOfScrips(scripCode : Long, scripId : String, scripName : String, status : String, group: String,
  faceValue : Long, ISINNo : String, industry: String, instrument: String) {

}
