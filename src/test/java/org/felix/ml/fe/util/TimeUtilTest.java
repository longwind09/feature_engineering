package org.felix.ml.fe.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @version 2016年12月9日 下午3:56:58
 */
@RunWith(JUnit4.class)
public class TimeUtilTest {
    public static DateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    @Test
    public void test() throws ParseException {
        String time = "2016-12-09-15-58-20";
        Date date = format.parse(time);
        String ret = TimeUtil.getTimeFloor(date, 10);
        Assert.assertEquals("2016-12-09-15-50", ret);
    }
}
