package org.felix.ml.fe.util;

import org.apache.commons.lang.StringUtils;

/**
 *
 *
 */

/**
 * 这个类的作用是？
 * 是为了比较java和python对浮点数的格式化行为是否一致吗？
 */
public class PythonUtil {
    public static String format(double f, int num) {
        String str = String.format("%." + num + "f", f);
        String strTripEnd = StringUtils.stripEnd(str, "0");
        if (strTripEnd.endsWith(".")) {
            if ("0.".equals(strTripEnd))
                return "0";
            return strTripEnd + "0";
        } else
            return strTripEnd;
    }
}
