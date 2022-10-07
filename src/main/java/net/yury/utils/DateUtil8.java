package net.yury.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtil8 {
    public static final DateTimeFormatter DATE_FORMATTER_STRING = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static LocalDate parseDateStr(String dateStr) {
        return LocalDate.parse(dateStr, DATE_FORMATTER_STRING);
    }
    public static String formatDateStr(LocalDate date) {
        return date.format(DATE_FORMATTER_STRING);
    }
    public static LocalDate toLocalDate(Date date) {
        Instant instant = date.toInstant();
        return instant.atZone(ZoneOffset.of("+8")).toLocalDate();
    }
    public static Date toDate(LocalDate date) {
        return new Date(date.toEpochDay() * 24 * 60 * 60 * 1000);
    }
    public static java.sql.Date toSqlDate(LocalDate date) {
        return new java.sql.Date(date.toEpochDay() * 24 * 60 * 60 * 1000);
    }

    public static LocalDate getWeekStart(LocalDate date) {
        int value = date.getDayOfWeek().getValue();
        return date.minusDays(value - 1);
    }
    public static LocalDate getWeekEnd(LocalDate date) {
        int value = date.getDayOfWeek().getValue();
        return date.plusDays(7 - value);
    }

    public static LocalDate getMonthStart(LocalDate date) {
        return date.withDayOfMonth(1);
    }
    public static LocalDate getMonthEnd(LocalDate date) {
        return date.withDayOfMonth(1).plusMonths(1).minusDays(1);
    }

    public static LocalDate getYearStart(LocalDate date) {
        return date.withDayOfYear(1);
    }
    public static LocalDate getYearEnd(LocalDate date) {
        return date.withDayOfYear(1).plusYears(1).minusDays(1);
    }
}
