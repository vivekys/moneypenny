package com.moneypenny.util

import com.gargoylesoftware.htmlunit.html._

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Created by vives on 1/27/15.
 */
object DomUtil {

  def findAllChildren (parent : DomNode) : mutable.ListBuffer[HtmlElement] = {
    val result = mutable.ListBuffer.empty[HtmlElement]
    for (child <- parent.getChildNodes) {
      if (child.isInstanceOf[HtmlElement]) {
        result += child.asInstanceOf[HtmlElement]
      }
    }
    for (child <- parent.getChildNodes) {
      result ++= findAllChildren(child)
    }
    result
  }

  def findTableElement (page : HtmlPage, text : String) : Option[HtmlTable] = {
    val children = findAllChildren(page)
    for (child <- children) {
      if (child.asText().contains(text) && child.isInstanceOf[HtmlTable]) {
        return Some(child.asInstanceOf[HtmlTable])
      }
    }
    return None
  }
}
