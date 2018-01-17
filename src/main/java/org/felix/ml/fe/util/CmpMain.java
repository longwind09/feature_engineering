package org.felix.ml.fe.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 *
 *
 */
public class CmpMain {
    public static void main(String[] args) throws IOException {
        File scp = new File(args[0]);
        List<String> files = IOUtils.readLines(new FileReader(scp));
        for (String file : files) {
            String[] name = StringUtils.split(file, "/");
            String fname = name[name.length - 1];
            LineIterator iter = IOUtils.lineIterator(new FileReader(file));
            int id = 0;
            while (iter.hasNext()) {
                id++;
                String line = iter.next();
                System.out.println(String.format("%s %s_%s", DigestUtils.md5Hex(line), fname, id));
            }
        }
    }
}
