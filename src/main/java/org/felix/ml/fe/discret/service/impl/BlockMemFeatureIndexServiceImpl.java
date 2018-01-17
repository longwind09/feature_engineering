package org.felix.ml.fe.discret.service.impl;

import org.felix.ml.fe.discret.service.IFeatureIndexService;
import org.felix.ml.fe.discret.service.UnsupportException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 *
 */
public class BlockMemFeatureIndexServiceImpl implements IFeatureIndexService {
    public long pos[];
    public byte len[];
    private MemBytes memBytes = new MemBytes();
    private int num = 0;
    private long length = 0;

    public static int unsignedToBytes(byte a) {
        int b = a & 0xFF;
        return b;
    }

    public void init(File dicFile) throws FileNotFoundException, IOException, UnsupportException {
//		testMaxAlloc();
        LineIterator iter = IOUtils.lineIterator(new FileInputStream(dicFile), "utf-8");
        while (iter.hasNext()) {
            String key = iter.next();
            if (StringUtils.isEmpty(key))
                continue;
            collectString(key);
            if (length < 0)
                throw new UnsupportException("length<0");
        }
        iter.close();
        memBytes.alloc(this.length);
        this.pos = new long[this.num];
        this.len = new byte[this.num];
        iter = IOUtils.lineIterator(new FileInputStream(dicFile), "utf-8");
        int idCur = 0;
        long dataCur = 0;
        while (iter.hasNext()) {
            String key = iter.next();
            if (StringUtils.isEmpty(key))
                continue;
            long nextCur = addKey(key, dataCur, idCur);
            idCur++;
            dataCur = nextCur;
        }
        iter.close();
    }

    //不同的jvm需要测试一下最大能申请的内存
    public void testMaxAlloc() {
        byte[] bytes;
        int try_num = 32;
        int num = Integer.MAX_VALUE / try_num;
        for (int i = 0; i <= try_num; i++) {
            try {
                bytes = new byte[i * num];
            } catch (Throwable e) {
                throw new OutOfMemoryError(String.format("alloc %s %s error!", i, i * num));
            }
        }
    }

    public long addKey(String line, long dataCur, int idCur) {
        String[] arrs = StringUtils.split(line, "\t", 2);
        String key = arrs[0];
        this.pos[idCur] = dataCur;
        this.len[idCur] = (byte) key.length();
        for (int i = 0; i < key.length(); i++) {
            memBytes.set(dataCur + i, (byte) key.charAt(i));
        }
        return dataCur + key.length();
    }

    /**
     * @param line
     * @return
     * @throws UnsupportException
     */
    private long collectString(String line) throws UnsupportException {
        if (length < 0)
            return length;
        String[] arrs = StringUtils.split(line, "\t", 2);
        String key = arrs[0];
        if (key.length() >= 127) {
            throw new UnsupportException(String.format("length>=127 [%s] [%s]", key, key.length()));
        }
        num++;
        for (int i = 0; i < key.length(); i++) {
            if (!CharUtils.isAscii(key.charAt(i))) {
                throw new UnsupportException(String.format("not ascii [%s] [%s]", key, key.charAt(i)));
            }
        }
        length += key.length();
        return length;
    }

    /**
     * 自己实现的二分搜索
     *
     * @param key
     * @return
     */
    public int search(String key) {
        if (key.length() >= 128) {
            return -1;
        }
        //特征取值不为ascii，则返回-1，没找到
        for (int i = 0; i < key.length(); i++) {
            if (!CharUtils.isAscii(key.charAt(i))) {
                return -1;
            }
        }
        //把特征名及取值组成的key变成字节数组
        byte[] source = new byte[key.length()];
        for (int i = 0; i < key.length(); i++) {
            source[i] = (byte) key.charAt(i);
        }
        //high 最大为size-1
        int high = this.num - 1;
        //low 从0 开始
        int low = 0;
        int mid;
        while (low <= high) {
            mid = low + (high - low) / 2;
            int cmpRet = cmp(source, mid);
            if (cmpRet > 0)
                low = mid + 1;
            else if (cmpRet < 0)
                high = mid - 1;
            else
                return mid;
        }
        return -1;
    }

    /**
     * 自己实现的字符串比较函数
     *
     * @param l
     * @param index
     * @return
     */
    private int cmp(byte[] l, int index) {
        int len1 = l.length;
        int len2 = len[index];
        long start = pos[index];
        int lim = Math.min(len1, len2);

        int k = 0;
        while (k < lim) {
            int c1 = unsignedToBytes(l[k]);
            int c2 = unsignedToBytes(getByte(start + k));
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }

    public int size() {
        return this.num;
    }

    public String get(int index) {
        long start = this.pos[index];
        int len = this.len[index];
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) getByte(start + i);
        }
        return new String(chars);
    }

    private byte getByte(long pos) {
        return memBytes.getByte(pos);
    }
}
