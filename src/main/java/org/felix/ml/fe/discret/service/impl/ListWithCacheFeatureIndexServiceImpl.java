package org.felix.ml.fe.discret.service.impl;

import org.felix.ml.fe.discret.service.IFeatureIndexService;
import org.felix.ml.fe.discret.service.UnsupportException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 *
 *
 */
public class ListWithCacheFeatureIndexServiceImpl implements IFeatureIndexService {
    public static final int DEFAULT_L1_CACHE_SIZE = 1000 * 1;
    public static final int DEFAULT_L2_CACHE_SIZE = 1000 * 10;
    public static final int DEFAULT_L3_CACHE_SIZE = 1000 * 100;
    private static Logger info = Logger.getLogger("info");
    private static Logger warn = Logger.getLogger("warn");
    public int cache_L1_Num = 0;
    public int cache_L2_Num = 0;
    public int cache_L3_Num = 0;
    public int featsNum = 0;
    public int miss = 0;
    private List<String> featsDicList = new ArrayList<String>();
    private Map<String, Integer> l1_cache = new HashMap<String, Integer>(DEFAULT_L1_CACHE_SIZE);
    private Map<String, Integer> l2_cache = new HashMap<String, Integer>(DEFAULT_L2_CACHE_SIZE);
    private Map<String, Integer> l3_cache = new HashMap<String, Integer>(DEFAULT_L3_CACHE_SIZE);

    public void init(File dicFile) throws FileNotFoundException, IOException, UnsupportException {
        featsDicList = new ArrayList<String>(1000 * 1000);
        int id = 1;
        LineIterator iter = IOUtils.lineIterator(new FileInputStream(dicFile), "utf-8");
        Map<String, Integer> treeMap = new TreeMap<String, Integer>(new Comparator<String>() {
            public int compare(String o1, String o2) {
                String[] arr1 = StringUtils.split(o1, ":", 2);
                String[] arr2 = StringUtils.split(o2, ":", 2);
                int n1 = Integer.parseInt(arr1[0]);
                int n2 = Integer.parseInt(arr2[0]);
                if (n1 != n2)
                    return n1 - n2;
                return arr2[1].compareTo(arr1[1]);
            }
        });
        while (iter.hasNext()) {
            String line = iter.next();
            if (StringUtils.isEmpty(line))
                continue;
            String[] arrs = StringUtils.split(line, "\t", 2);
            if (StringUtils.isEmpty(arrs[0]))
                continue;
            id++;
            if (treeMap.size() <= DEFAULT_L3_CACHE_SIZE) {
                String mapKey = String.format("%s:%s", arrs[1], arrs[0]);
                Integer r = treeMap.put(mapKey, id);
                if (r != null) {
                    info.info(String.format("at:%s %s", id, mapKey));
                }
            } else {
                String lkey = treeMap.keySet().iterator().next();
                String[] arr1 = StringUtils.split(lkey, ":", 2);
                int num = Integer.parseInt(arr1[0]);
                int cnum = Integer.parseInt(arrs[1]);
                if (cnum >= num) {
                    treeMap.put(String.format("%s:%s", arrs[1], arrs[0]), id);
                    lkey = treeMap.keySet().iterator().next();
                    treeMap.remove(lkey);
                }
            }
            featsDicList.add(arrs[0]);
            if (id % (1000 * 1000) == 0) {
                Runtime runtime = Runtime.getRuntime();
                info.info("load at:" + id);
                info.info(String.format("total:%s free:%s", runtime.totalMemory(), runtime.freeMemory()));
            }
        }
        iter.close();
        int totalSize = treeMap.keySet().size();
        Iterator<String> iterStr = treeMap.keySet().iterator();
        int index = 0;
        while (iterStr.hasNext()) {
            index++;
            String key = iterStr.next();
            String[] arrs = StringUtils.split(key, ":", 2);
            int aid = Integer.parseInt(arrs[0]);
            if ((totalSize - index) <= DEFAULT_L1_CACHE_SIZE) {
                l1_cache.put(arrs[1], aid);
            } else if ((totalSize - index) <= DEFAULT_L2_CACHE_SIZE) {
                l2_cache.put(arrs[1], aid);
            } else if ((totalSize - index) <= DEFAULT_L3_CACHE_SIZE) {
                l3_cache.put(arrs[1], aid);
            } else {
                break;
            }
        }
        String lk1 = l1_cache.keySet().iterator().next();
        String lk2 = l2_cache.keySet().iterator().next();
        String lk3 = l3_cache.keySet().iterator().next();
        info.info(String.format("finish load, tmap:%s size:%s,%s,%s last:%s,%s,%s", treeMap.size(), l1_cache.size(),
                l2_cache.size(), l3_cache.size(), lk1, lk2, lk3));
    }

    public int size() {
        return featsDicList.size();
    }

    public int search(String key) {
        featsNum++;
        Integer ret = l1_cache.get(key);
        if (ret != null) {
            cache_L1_Num++;
            return ret;
        }
        ret = l2_cache.get(key);
        if (ret != null) {
            cache_L2_Num++;
            return ret;
        }
        ret = l3_cache.get(key);
        if (ret != null) {
            cache_L3_Num++;
            return ret;
        }
        miss++;
        ret = Collections.binarySearch(featsDicList, key);
        return ret;
    }

    public String get(int index) {
        return featsDicList.get(index);
    }

}
