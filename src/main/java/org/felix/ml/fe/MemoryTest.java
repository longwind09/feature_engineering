package org.felix.ml.fe;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 *
 */
public class MemoryTest {
    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
        List<String> featsDicList = new ArrayList<String>();

        Iterator<String> iter = IOUtils.lineIterator(new FileInputStream(new File(args[0])), "utf-8");
        long id = 1l;
        while (iter.hasNext()) {
            String line = iter.next();
            if (StringUtils.isEmpty(line))
                continue;
            String[] arrs = StringUtils.split(line, "\t", 2);
            if (StringUtils.isEmpty(arrs[0]))
                continue;
            featsDicList.add(StringUtils.trim(arrs[0]));
            id++;
            if (id % (1000 * 1000) == 0) {
                Runtime runtime = Runtime.getRuntime();
                System.out.println("load at:" + id);
                System.out.println(String.format("total:%s free:%s", runtime.totalMemory(), runtime.freeMemory()));
            }
        }
        Runtime runtime = Runtime.getRuntime();
        System.out.println("finish load");
        System.out.println(String.format("total:%s free:%s", runtime.totalMemory(), runtime.freeMemory()));
        for (int i = 0; i <= 120; i++) {
            System.gc();
            Thread.currentThread().sleep(1000 * 5);
            System.gc();
            runtime = Runtime.getRuntime();
            System.out.println(String.format("total:%s free:%s", runtime.totalMemory(), runtime.freeMemory()));

        }
    }
}
