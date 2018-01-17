package org.felix.ml.fe.discret.service.impl;

import org.felix.ml.fe.discret.service.IFeatureIndexService;
import org.felix.ml.fe.discret.service.UnsupportException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Assert;

import java.io.*;
import java.util.Date;
import java.util.Iterator;

/**
 *
 * 4
 */
//@RunWith(JUnit4.class)
public class MemoryUseCompare {
    public static final String hugeListFile = "e://bigDic.txt";
    final String key = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIGKLMNOPQRSTUVWXYZ.-+*~/";
    char[] chars = key.toCharArray();

    //	@Test
    public void testListImpl() throws FileNotFoundException, IOException, UnsupportException, InterruptedException {
        IFeatureIndexService featureIndexService = new ListFeatureIndexServiceImpl();
        long start = new Date().getTime();
        featureIndexService.init(new File(hugeListFile));
        long end = new Date().getTime();
        System.out.println("finish load+" + (end - start) / (1000));
        printMemoryUsage();
        long findStart = new Date().getTime();
        check(featureIndexService);
        long findEnd = new Date().getTime();
        System.out.println("finish check+" + (findEnd - findStart) / (1000) + " class:" + featureIndexService.getClass().getSimpleName());
    }

    //	@Test
    public void testBlockMemImpl() throws FileNotFoundException, IOException, UnsupportException, InterruptedException {
        long start = new Date().getTime();
        IFeatureIndexService featureIndexService = new BlockMemFeatureIndexServiceImpl();
        featureIndexService.init(new File(hugeListFile));
        long end = new Date().getTime();
        System.out.println("finish load+" + (end - start) / (1000));
        printMemoryUsage();
        long findStart = new Date().getTime();
        check(featureIndexService);
        long findEnd = new Date().getTime();
        System.out.println("finish check+" + (findEnd - findStart) / (1000) + " class:" + featureIndexService.getClass().getSimpleName());
    }

    //	@Test
    public void getDicFile() throws IOException {
        int num = 5 * 1000 * 1000;
        int lenMin = 4;
        int lenMax = 25;
        int keyLen = key.length();
        OutputStream outStream = new BufferedOutputStream(new FileOutputStream(hugeListFile));
        for (int i = 0; i < num; i++) {
            String id = Long.toString(i, Character.MAX_RADIX - 1);
            String fid = StringUtils.leftPad(id, 6, "0");
            int len = RandomUtils.nextInt((lenMax - lenMin)) + lenMin;
            StringBuffer sb = new StringBuffer();
            for (int j = 0; j <= len; j++) {
                sb.append(chars[RandomUtils.nextInt(keyLen)]);
            }
            String out = String.format("%s_%s\t%s\n", fid, sb.toString(), i);
            IOUtils.write(out, outStream);
        }
        outStream.close();
    }

    public void check(IFeatureIndexService featureIndexService) throws IOException {
        Iterator<String> iter = IOUtils.lineIterator(new FileInputStream(new File(hugeListFile)), "utf-8");
        while (iter.hasNext()) {
            String key = iter.next();
            String[] arrs = StringUtils.split(key, "\t");
            int index = featureIndexService.search(arrs[0]);
            Assert.assertTrue(index >= 0);
            String find = featureIndexService.get(index);
            Assert.assertEquals(find, arrs[0]);
        }
        String unfind = "unfind";
        int index = featureIndexService.search(unfind);
        Assert.assertTrue(index < 0);
    }

    private void printMemoryUsage() throws InterruptedException {
        for (int i = 1; i <= 5; i++) {
            System.gc();
            Thread.sleep(1000 * 3);
            Runtime runtime = Runtime.getRuntime();
            System.out.println(String.format("total:%s free:%s", runtime.totalMemory(), runtime.freeMemory()));
        }
    }
}
