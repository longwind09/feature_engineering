package org.felix.ml.fe.discret.service.impl;

import org.felix.ml.fe.discret.service.IFeatureIndexService;
import org.felix.ml.fe.discret.service.UnsupportException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 *
 *          <p>
 *          用list来对特征取值集合进行编号,list大小容易爆内存
 */
public class ListFeatureIndexServiceImpl implements IFeatureIndexService {
    private List<String> featsDicList = new ArrayList<String>();

    public void init(File dicFile) throws FileNotFoundException, IOException, UnsupportException {
        //总行数
        int num = getNum(dicFile);
        featsDicList = new ArrayList<String>(num + 1);
        Iterator<String> iter = IOUtils.lineIterator(new FileInputStream(dicFile), "utf-8");
        while (iter.hasNext()) {
            String line = iter.next();
            if (StringUtils.isEmpty(line))
                continue;
            String[] arrs = StringUtils.split(line, "\t", 2);
            if (StringUtils.isEmpty(arrs[0]))
                continue;
            //只用了特征与取值，没有用出现次数
            featsDicList.add(arrs[0]);
        }
    }

    //获取文件行数
    private int getNum(File dicFile) throws FileNotFoundException, IOException {
        int ret = 0;
        LineIterator iter = IOUtils.lineIterator(new FileInputStream(dicFile), "utf-8");
        while (iter.hasNext()) {
            iter.next();
            ret++;
        }
        iter.close();
        return ret;
    }

    /**
     * 二叉查找,得到数组下标
     *
     * @param key
     * @return
     */
    public int search(String key) {
        return Collections.binarySearch(featsDicList, key);
    }

    /**
     * 取list大小
     *
     * @return
     */
    public int size() {
        return featsDicList.size();
    }

    /**
     * 按下标取值
     *
     * @param index
     * @return
     */
    public String get(int index) {
        return featsDicList.get(index);
    }
}
