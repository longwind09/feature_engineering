package org.felix.ml.fe.discret;

import org.felix.ml.fe.MapperException;
import org.felix.ml.fe.discret.service.IFeatureIndexService;
import org.felix.ml.fe.discret.service.UnsupportException;
import org.felix.ml.fe.discret.service.impl.BlockMemFeatureIndexServiceImpl;
import org.felix.ml.fe.util.FeatureTrans;
import org.felix.ml.fe.util.StringDecode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 *
 *
 *
 * 支持稀疏特征
 */
public class Process4BlockMem {
    public static final String dicFile = "dic";
    public static final String dicSortFile = "dic_sort";
    private static Logger info = Logger.getLogger("info");
    private static Logger warn = Logger.getLogger("warn");
    Properties prop = new Properties();
    int warnNumFeats = 0;
    Map<String, Long> warnNumMap = new HashMap<String, Long>();
    private List<String> singleList = new ArrayList<String>();
    private List<List<String>> combineList = new ArrayList<List<String>>();
    private List<String> combineKeys = new ArrayList<String>();
    private IFeatureIndexService service;
    private int featSize;
    private int mod = 100 * 1000;
    private int totalNum = 0;
    private int passNum = 0;
    private int warnNum = 0;

    public static void main(String[] args) throws FileNotFoundException, IOException {
        if (args.length != 2) {
            warn.error("argv error");
            warn.error("eg:inputScp outputScp");
            System.exit(-1);
        }
        Process4BlockMem p = new Process4BlockMem();
        p.setup();
        p.process(args[0], args[1]);
        p.print();
    }

    private void setup() throws FileNotFoundException, IOException {
        prop.load(new FileInputStream(new File("cfg.properties")));
        String single = (String) prop.get("single");
        String combine = (String) prop.get("merge");
        boolean singleWarn = StringDecode.setSingleList(single, ",", singleList);
        boolean combineWarn = StringDecode.setCombineList(combine, ",", "*", combineList, combineKeys);
        featSize = singleList.size() + combineList.size();
        service = new BlockMemFeatureIndexServiceImpl();
        try {
            service.init(new File(dicFile));
        } catch (UnsupportException e) {
            throw new IOException(e);
        }
        info.info(String.format("single:%s combine:%s dic_size:%s featSize:%s", single, combine, service.size(),
                featSize));
        if (singleWarn) {
            warn.warn("repeat single feature!");
        }
        if (combineWarn) {
            warn.warn("repeat combine feature!");
        }
    }

    public void process(String inScp, String outScp) throws FileNotFoundException, IOException {
        List<String> inScpList = IOUtils.readLines(new FileInputStream(inScp));
        List<String> outScpList = IOUtils.readLines(new FileInputStream(outScp));
        for (int i = 0; i < inScpList.size(); i++) {
            String inFile = inScpList.get(i);
            String outFile = outScpList.get(i);
            process(new File(inFile), new File(outFile));
        }
    }

    public void process(File inFile, File outFile) throws FileNotFoundException, IOException {
        InputStream in = new FileInputStream(inFile);
        Iterator<String> liter = IOUtils.lineIterator(in, "utf-8");
        try {
            Writer writer = new FileWriter(outFile);
            while (liter.hasNext()) {
                String line = liter.next();
                try {
                    if (totalNum % mod == 0) {
                        System.gc();
                        Runtime runtime = Runtime.getRuntime();
                        info.info(String.format("total:%s free:%s", runtime.totalMemory(), runtime.freeMemory()));
                    }
                    totalNum++;
                    if (totalNum % 10000 == 0)
                        writer.flush();
                    processLine(line, writer);
                    passNum++;
                } catch (MapperException e) {
                    warnNum++;
                }
            }
            writer.close();
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private void processLine(String line, Writer writer) throws MapperException, IOException {
        Map<String, String> map = StringDecode.str2Map(line, 2);
        Map<String, String> featsMap = FeatureTrans.trans(map, singleList, combineList, combineKeys);
//        if (featsMap.size() != featSize) {
//            throw new MapperException("Incomplete_feature");
//        }
        String[] arrs = StringUtils.split(line, " ", 2);
        List<String> feats = new ArrayList<String>();
        for (Map.Entry<String, String> entry : featsMap.entrySet()) {
            String kv = FeatureTrans.getKVString(entry.getKey(), entry.getValue());
            int index = service.search(kv);
            if (index < 0) {
                warn.warn(String.format("not find %s %s", totalNum, kv));
                addWarnCount(entry.getKey());
                continue;
            }
            feats.add(String.format("%s:%s", index, 1));
        }
        if (feats.size() == 0)
            throw new MapperException("empty_feature");
        writer.write(String.format("%s %s", arrs[0], StringUtils.join(feats, " ")));
        writer.write("\n");
    }

    public void addWarnCount(String key) {
        warnNumFeats++;
        Long num = warnNumMap.get(key);
        if (num == null)
            warnNumMap.put(key, 1l);
        else
            warnNumMap.put(key, num.longValue() + 1);
    }

    public void print() {
        info.info(String.format("total:%s pass:%s warn:%s", totalNum, passNum, warnNum));
        for (Map.Entry<String, Long> entry : warnNumMap.entrySet()) {
            info.info(String.format("%s\t%s", entry.getKey(), entry.getValue()));
        }
    }
}
