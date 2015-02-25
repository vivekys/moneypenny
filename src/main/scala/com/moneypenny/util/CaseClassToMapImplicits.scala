package com.moneypenny.util

/**
 * Created by vives on 2/25/15.
 */
object CaseClassToMapImplicits {
  implicit class CaseClassToMap(c: AnyRef) {
    def toStringWithFields = {
      (Map[String, Any]() /: c.getClass.getDeclaredFields) { (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(c))
      }
    }
  }
}
