package com.moneypenny.util

import java.util.Date

import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate, LocalDateTime, LocalTime}
import org.slf4j.LoggerFactory

/**
 * Created by vives on 3/1/15.
 */
object RunableDates {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  def getStartAndEndDates(lastRun : Date) = {
    val dateFormatToRun = DateTimeFormat.forPattern("dd/MM/YYYY")
    val currentDateTime = new LocalDateTime(new Date())
    val startDate = if (lastRun != null) {
      val localDate = new LocalDate(lastRun)
      val nextRunDate =   if (localDate.plusDays(1).toLocalDateTime(new LocalTime(18, 0, 0)).compareTo(currentDateTime) < 0)
        localDate.plusDays(1)
      else
        localDate
      dateFormatToRun.print(nextRunDate)
    } else
      "01/01/1990"



    val endDate = if (currentDateTime.getHourOfDay >= 18)
        dateFormatToRun.print(currentDateTime)
      else
        dateFormatToRun.print(currentDateTime.minusDays(1))

    logger.info("Start Date - " + startDate +
      " and End Date - " + endDate)

    (startDate, endDate)
  }
}
