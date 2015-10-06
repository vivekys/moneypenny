package com.moneypenny.util

import java.text.NumberFormat

import com.gargoylesoftware.htmlunit.html.HtmlTable

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * Created by vives on 1/1/15.
 */
object ExtractFromTable {
  def extractFromHtmlTable (table : HtmlTable) = {
    var startRow = 0
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[String, Map[String, Option[Number]]]
    val columnMap = scala.collection.mutable.LinkedHashMap.empty[Int, String]
    breakable {
      for (i <- 0 until table.getRows.length) {
        startRow = i
          val row = table.getRows.get(i)
          val height = row.getAttribute("height")
          if (!height.contains("1px"))
            break()
      }
    }

    val columns = table.getRow(startRow).getCells
    for (col <- columns) {
      if (!col.asText().isEmpty)
        columnMap.put(col.getIndex, col.asText())
    }

    for (i <- (startRow + 1) until table.getRows.length) {
      if (! table.getRows.get(i).getCells.get(0).asText().isEmpty) {
        val rowName = table.getRows.get(i).getCells.get(0).asText().replaceAllLiterally(".", "")
        val colValues = ArrayBuffer.empty[(Int, String)]
        for (j  <- 1 until table.getRows.get(i).getCells.length) {
          if (! table.getRows.get(i).getCells.get(j).asText().isEmpty) {
            colValues += ((table.getRows.get(i).getCells.get(j).getIndex, table.getRows.get(i).getCells.get(j).asText()))
          }
        }
        if (!colValues.isEmpty) {
          for (vls <- colValues) {
            val dataMap = scala.collection.mutable.LinkedHashMap.empty[String, Option[Number]]
            val nf = NumberFormat.getInstance
            val numericValue = try {
              Some(nf.parse(vls._2))
            } catch {
              case ex : Exception => None
            }
            dataMap.put(rowName, numericValue)
            val value = returnMap.get(columnMap.get(vls._1).get) match {
              case Some(data) =>  dataMap ++ data
              case None => dataMap
            }
            returnMap.put(columnMap.get(vls._1).get, value.toMap)
          }
        }
      }
    }
    returnMap
  }
}
