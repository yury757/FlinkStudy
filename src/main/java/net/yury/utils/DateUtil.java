package net.yury.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    public static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final long ONE_DAY_SEC = 86400000L;

    public static synchronized final String formatToString(Date date) {
        return FORMAT.format(date);
    }
    public static synchronized final Date parseFromString(String dateString) throws ParseException {
        return FORMAT.parse(dateString);
    }
}
