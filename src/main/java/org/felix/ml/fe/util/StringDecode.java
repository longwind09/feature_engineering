package org.felix.ml.fe.util;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.util.*;

import static org.apache.commons.lang.StringUtils.*;

/**
 *
 *
 */
public class StringDecode {

    /**
     * 解析一行样本
     *
     * @param in
     * @param start
     * @return
     */
    public static Map<String, String> str2Map(String in, int start) {
        if (in == null)
            return null;
        Map<String, String> ret = new TreeMap<String, String>();
        String[] items = StringUtils.split(in, null);
        for (int i = start; i < items.length; i++) {
            String item = items[i];
            String[] arr = StringUtils.splitPreserveAllTokens(item, ":", 2);
            if (arr.length == 2)
                ret.put(arr[0], arr[1]);
        }
        return ret;
    }

    /**
     * @param fields 解析离散化配置文件下方的特征选择配置，其中但特征single部分解析
     * @param separatorChars
     * @param list
     * @return 返回值表示是否有单特征配置重复了
     */
    public static boolean setSingleList(String fields, String separatorChars, List<String> list) {
        String[] arrs = split(fields, separatorChars);
        Set<String> keys = new HashSet<String>();
        boolean repeat = false;
        for (String str : arrs) {
            if (isEmpty(str))
                continue;
            str = trim(str);
            if (keys.contains(str)) {
                repeat = true;
                continue;
            }
            keys.add(str);
            list.add(trim(str));
        }
        return repeat;
    }

    /**
     * 解析组合特征
     * @param fields 特征离散化配置下方的组合区merge
     * @param separatorChars  组合特征间的分隔符，一般用逗号
     * @param innerSeparatorChars 组合特征表示法，特征A*特征B
     * @param list  组合特征列表,里面的组合特征间先后顺序是配置顺序，但是单个组合特征各项顺序是字典序
     * @param keys 配置的组合特征, A*B  B*A 是重复的
     * @return
     */
    public static boolean setCombineList(String fields, String separatorChars, String innerSeparatorChars,
                                         List<List<String>> list, List<String> keys) {
        String[] arrs = split(fields, separatorChars);
        //唯一化,101+601<=>601+101
        List<String> uniqKeys = new ArrayList<String>();
        boolean repeat = false;
        for (String str : arrs) {
            if (isEmpty(str))
                continue;
            str = trim(str);
            String[] items = split(str, innerSeparatorChars);
            //可以是多项组合
            List<String> mixFeature = new ArrayList<String>();
            for (String aitem : items) {
                if (isEmpty(aitem))
                    continue;
                mixFeature.add(aitem);
            }
            if (mixFeature.size() == 0)
                continue;

            //组合特征先后顺序通过排序唯一确定,因为不允许两个相同特征出现，或者两个相同特征组合
            List<String> mixFeatureCLone = new ArrayList<String>();
            mixFeatureCLone.addAll(mixFeature);
            Collections.sort(mixFeatureCLone, new Comparator<String>() {

                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }

            });
            String strKey = StringUtils.join(mixFeatureCLone, "+");
            if (uniqKeys.contains(strKey)) {
                repeat = true;
                continue;
            }
            uniqKeys.add(strKey);

            //以输入得顺序为主
            strKey = StringUtils.join(mixFeature, "+");
            keys.add(strKey);
            list.add(mixFeature);
        }
        return repeat;
    }

    public static Map<String, Map<String, Integer>> decode(String line) {
        Map<String, Map<String, Integer>> ret = new HashMap<String, Map<String, Integer>>();
        String[] items = StringUtils.split(line, " ");
        for (String item : items) {
            String[] pair = StringUtils.split(item, ":", 2);
            String key = pair[0];
            String value = pair[1];
            JSONObject json = new JSONObject(value);
            Iterator<String> iter = json.keys();
            Map<String, Integer> map = new HashMap<String, Integer>();
            while (iter.hasNext()) {
                String k = iter.next();
                int cnt = (Integer) json.get(k);
                map.put(k, cnt);
            }
            ret.put(key, map);
        }
        return ret;
    }
}
