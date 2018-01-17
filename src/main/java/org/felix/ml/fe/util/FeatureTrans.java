package org.felix.ml.fe.util;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 *
 */
public class FeatureTrans {
    /**
     * 统一单特征和组合特征
     *
     * 组合特征key是组合特征用加号连接，value是它们对应的值用加号连接
     *
     * @param map
     * @param single
     * @param combineList
     * @param combineKeys
     * @return
     */
    public static Map<String, String> trans(Map<String, String> map, List<String> single, List<List<String>> combineList,
                                            List<String> combineKeys) {
        Map<String, String> ret = new TreeMap<String, String>();
        //遍历单特征,只取use里面对应的single
        for (String key : single) {
            if (map.containsKey(key)) {
                String value = map.get(key);
                ret.put(key, value);
            }
        }
        //遍历组合特征,组合特征的value变成对应单特征value的组合
        int size = combineList.size();
        for (int i = 0; i < size; i++) {
            List<String> feats = combineList.get(i);
            String key = combineKeys.get(i);
            List<String> v = new ArrayList<String>();

            //这里考虑了组合特征对应的特征值并不存在的情况，因为咱们的特征向量现在支持稀疏向量
            //和leader讨论过，对于这种情况的结论是去掉。
            boolean skip = false;
            for (String feat : feats) {
                if (map.containsKey(feat)) {
                    v.add(map.get(feat));
                }else{
                    skip = true;
                    break;
                }
            }
            if(!skip){
                ret.put(key, StringUtils.join(v, "+"));
            }
        }
        return ret;
    }

    /**
     * cachemap 里存的是每个特征与取值组合对应出现的次数,如果去重是不是就是全局唯一编号了？？
     *
     * @param cacheMap
     * @param featsMap
     */
    public static void merge(Map<String, Long> cacheMap, Map<String, String> featsMap) {
        for (Map.Entry<String, String> entry : featsMap.entrySet()) {
            String kv = getKVString(entry.getKey(), entry.getValue());
            Long num = cacheMap.get(kv);
            //第一次
            if (num == null)
                cacheMap.put(kv, 1l);
            else
                //更改其值
                cacheMap.put(kv, num.longValue() + 1);
        }
    }

    /**
     * 把特征、取值用星号连起来
     *
     * @param key
     * @param value
     * @return
     */
    public static String getKVString(String key, String value) {
        return String.format("%s*%s", key, value);
    }
}
