package org.felix.ml.fe.normalize;

import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.normalize.cnf.CnfLoad;
import org.felix.ml.fe.normalize.cnf.CnfModel;
import org.felix.ml.fe.normalize.util.ConvertUtil;
import org.felix.ml.fe.util.ProxyMain;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.List;

/**
 *
 * 9
 */
public class Process {
    private static Logger info = Logger.getLogger("info");
    private static Logger warn = Logger.getLogger("warn");

    public static void main(String[] args) throws FileNotFoundException, IOException, ConfigException {
        if (args.length < 3) {
            warn.error("usage:");
            warn.error("Process cnf.txt in.txt out.txt");
            System.out.println("Process cnf.txt in.txt out.txt");
            System.exit(-1);
        }
        PropertyConfigurator.configure(ProxyMain.class.getResourceAsStream("/sample_log4j.properties"));
        CnfModel cnfModel = CnfLoad.load(new File(args[0]));
        List<String> lines = IOUtils.readLines(new FileReader(new File(args[1])));
        FileWriter writer = new FileWriter(new File(args[2]));
        int total = 0;
        int pass = 0;
        int fail = 0;
        for (String line : lines) {
            total++;
            try {
                String ret = ConvertUtil.convert(line, cnfModel);
                IOUtils.write(String.format("%s\n", ret), writer);
                pass++;
            } catch (Throwable e) {
                warn.warn(line);
                warn.equals(e);
                fail++;
            }
        }
        writer.close();
        info.info("finish process");
        info.info(String.format("total:%s pass:%s fail:%s", total, pass, fail));
    }
}
