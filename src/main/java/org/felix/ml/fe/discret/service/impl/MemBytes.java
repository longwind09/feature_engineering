package org.felix.ml.fe.discret.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 *
 */
public class MemBytes {
    private List<byte[]> memorys = new ArrayList<byte[]>();
    //hadoop测试的安全数据
    private int maxSize = 4 * (Integer.MAX_VALUE / 32);

    public void alloc(long size) {
        memorys.clear();
        int num = (int) (size / maxSize);
        int mod = (int) (size % maxSize);
        for (int i = 0; i < num; i++) {
            try {
                byte[] bytes = new byte[maxSize];
                memorys.add(bytes);
            } catch (Throwable e) {
                Map map = System.getenv();
                Iterator it = map.entrySet().iterator();
                StringBuffer sb = new StringBuffer();
                while (it.hasNext()) {
                    Entry entry = (Entry) it.next();
                    sb.append(String.format("%s=%s\n", entry.getKey(), entry.getValue()));
                }
                Runtime runtime = Runtime.getRuntime();
                throw new OutOfMemoryError(String.format("alloc memory error! %s %s total:%s free:%s", i, maxSize,
                        runtime.totalMemory(), runtime.freeMemory()));
            }
        }
        try {
            byte[] bytes = new byte[mod];
            memorys.add(bytes);
        } catch (Throwable e) {
            throw new OutOfMemoryError(String.format("alloc memory error! %s %s", mod, maxSize));
        }
    }

    public void set(long size, byte value) {
        int num = (int) (size / maxSize);
        int mod = (int) (size % maxSize);
        byte[] bytes = memorys.get(num);
        bytes[mod] = value;
    }

    public byte getByte(long pos) {
        int num = (int) (pos / maxSize);
        int mod = (int) (pos % maxSize);
        byte[] bytes = memorys.get(num);
        return bytes[mod];
    }
}
