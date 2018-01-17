package org.felix.ml.fe.util;

import org.apache.commons.lang.time.DateUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @version 2016年12月9日 下午3:45:11
 */
public class TimeUtil {
    public static DateFormat MIN_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    public static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    public static List<String> listTimeFloor(Date time, int interval, int num) {
        List<String> ret = new ArrayList<String>();
        Date subTime = findNearTime(time, interval);
        Date truncateMin = DateUtils.truncate(subTime, Calendar.MINUTE);
        for (int i = 0; i < num; i++) {
            Date tmp = DateUtils.addMinutes(truncateMin, -1 * i * interval);
            ret.add(MIN_FORMAT.format(tmp));
        }
        return ret;
    }

    public static String getTimeFloor(Date time, int interval) {
        Date subTime = findNearTime(time, interval);
        Date truncateMin = DateUtils.truncate(subTime, Calendar.MINUTE);
        return MIN_FORMAT.format(truncateMin);
    }

    public static Date findNearTime(Date time, int interval) {
        int min = time.getMinutes();
        int mod = min % interval;
        Date subTime = DateUtils.addMinutes(time, -1 * mod);
        return subTime;
    }

    public static String getDate(Date time) {
        return DATE_FORMAT.format(time);
    }

    public static Date getDate(String dateStr) throws ParseException {
        return DATE_FORMAT.parse(dateStr);
    }

    public static Date getTime(String time) throws ParseException {
        return MIN_FORMAT.parse(time);
    }

    public static String getTime(Date time) {
        return MIN_FORMAT.format(time);
    }

    public static String getTime(long time) {
        return MIN_FORMAT.format(new Date(time));
    }
}
