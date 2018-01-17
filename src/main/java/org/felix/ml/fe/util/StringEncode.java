package org.felix.ml.fe.util;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 *
 */
public class StringEncode {
    public static String encode(Map<String, Map<String, Integer>> docCount,
                                Map<String, Map<String, Integer>> queryCount,
                                List<String> docList,
                                List<String> queryList) {
        List<String> retList = new ArrayList<String>();
        for (String field : docList) {
            retList.add(encode(field, docCount.get(field)));
        }
        for (String field : queryList) {
            retList.add(encode(field, queryCount.get(field)));
        }
        return StringUtils.join(retList, " ");
    }

    public static String encode(String field, Map<String, Integer> countMap) {
        if (countMap == null)
            return String.format("%s:{}", field);
        JSONObject json = new JSONObject(countMap);
        return String.format("%s:%s", field, json.toString());
    }
}
