package com.moneypenny.util

import com.gargoylesoftware.htmlunit.html.HtmlTable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.util.control.Breaks._

/**
 * Created by vives on 1/1/15.
 */
object ExtractFromTable {
  def extractFromHtmlTable (table : HtmlTable) = {
    var startRow = 0
    var canInsert = false
    val returnMap = scala.collection.mutable.LinkedHashMap.empty[(String, String), String]
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
        val rowName = table.getRows.get(i).getCells.get(0).asText()
        val colValues = ArrayBuffer.empty[(Int, String)]
        for (j  <- 1 until table.getRows.get(i).getCells.length) {
          if (! table.getRows.get(i).getCells.get(j).asText().isEmpty) {
            colValues += ((table.getRows.get(i).getCells.get(j).getIndex, table.getRows.get(i).getCells.get(j).asText()))
          }
        }
        if (!colValues.isEmpty) {
          for (vls <- colValues)
            returnMap.put((columnMap.get(vls._1).get, rowName), vls._2)
        }
      }
    }
    returnMap
  }
}
