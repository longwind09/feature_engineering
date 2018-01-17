package org.felix.ml.fe.util;

import org.felix.ml.fe.ParserException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.*;

/**
 *
 *
 */
public class StringParser {
    public static Map<String, String> parser2Map(String line, String invalidChars, List<String> checkFields) throws ParserException {
        return parser2Map(line, invalidChars, " ", checkFields);
    }

    public static Map<String, String> parser2Map(String line, String invalidChars, String separatorChars, List<String> checkFields) throws ParserException {
        return parser2Map(line, invalidChars, separatorChars, ":", checkFields);
    }

    public static Map<String, String> parser2Map(String line, String invalidChars, String separatorChars, String mapSeparatorChars, List<String> checkFields) throws ParserException {
        Map<String, String> ret = new HashMap<String, String>();
        String[] arrs = split(line, separatorChars);
        for (String pair : arrs) {
            String[] items = split(pair, mapSeparatorChars, 2);
            if (items.length != 2) {
                continue;
            }
            String key = trim(items[0]);
            if (!checkFields.contains(key) && !Constant.QID.equals(key))
                continue;
            if (ret.containsKey(key)) {
                throw new ParserException(ParserException.TYPE_REPEAT, key, String.format("error parser %s,repeat key!", pair));
            }
            String value = trim(items[1]);
            if (!isValid(value, invalidChars)) {
                throw new ParserException(ParserException.TYPE_INVALID_VALUE, key, String.format("error parser %s,invalid value!", pair));
            }
            ret.put(key, trim(items[1]));
        }
        return ret;
    }

    public static boolean isValid(String value, String invalidChars) {
        if (isEmpty(value))
            return false;
        if (containsAny(value, "~#|_"))
            return false;
        if (!isEmpty(invalidChars) && containsAny(value, invalidChars))
            return false;
        return true;
    }
}
