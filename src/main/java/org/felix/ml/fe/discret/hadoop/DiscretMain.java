package org.felix.ml.fe.discret.hadoop;

import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.hadoop.RowCounts;
import org.felix.ml.fe.normalize.cnf.CnfLoad;
import org.felix.ml.fe.normalize.cnf.CnfModel;
import org.felix.ml.fe.util.Constant;
import org.felix.ml.fe.util.HadoopUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 *
 */
public class DiscretMain {
    private static Logger info = Logger.getLogger("info");
    private static Logger warn = Logger.getLogger("warn");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException, ConfigException {
        Configuration conf = new Configuration();
        //---------------------------------------------
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length <= 1) {
            warn.error("argv error");
            warn.error("eg:workdir jobname=xxx&keytype=xxx&single=a,b,c&combine=e,f,g);");
            System.exit(-1);
        }
        String workPath = otherArgs[0];
        String config = otherArgs[1];
        HadoopUtil.initConf(conf, config, null);
        String type = conf.get("type", Constant.DEFAULT_TYPE_TRAIN);
        String inputPath = Constant.getPath(workPath, type, conf.get("indir", Constant.DEFAULT_NORM_DIR));
        String outPath = Constant.getPath(workPath, type, conf.get("outdir", Constant.DEFAULT_DISCRET_DIR));
        String cfgPath = Constant.getCnfPath(workPath, conf.get("cfg", Constant.DEFAULT_CNF));

        //----------------------------------------------------

        FileSystem fs = FileSystem.get(URI.create(cfgPath), conf);
        InputStream in = null;
        in = fs.open(new Path(cfgPath));
        CnfModel cnfModel = CnfLoad.load(in);
        conf.set("single", cnfModel.getSingle());
        conf.set("combine", cnfModel.getMerge());
        if (StringUtils.isEmpty(cnfModel.getSingle()) && StringUtils.isEmpty(cnfModel.getMerge())) {
            warn.error("both sinble & combine is empty!!");
            System.exit(-1);
        }
        //-------------------------------------------------------

        String jobName = conf.get("jobname");

        conf.set("mapred.create.symlink", "yes");
        DistributedCache.createSymlink(conf);
        String dicPath = Constant.getDicPath(workPath, Constant.DEFAULT_PART, type);
        String dicPath2 = Constant.getDicPath(workPath, Constant.DEFAULT_PART_2, type);
        FileSystem fstm = FileSystem.get(conf);
        //
        if (fstm.exists(new Path(dicPath2))) {
            warn.error("not support multi key_alloc!");
            System.exit(-1);
        }
        Path filePath = new Path(dicPath);
        String uriWithLink = filePath.toUri().toString() + "#" + DiscretMapper.dicFile;
        DistributedCache.addCacheFile(new URI(uriWithLink), conf);
        //--------------------------------------------------------------------------

        Job job = new Job(conf, jobName);
        HadoopUtil.setInput(conf, job, inputPath);
        job.setJarByClass(DiscretMain.class);
        job.setMapperClass(DiscretMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        MultipleOutputs.addNamedOutput(job, Constant.multiOutput, TextOutputFormat.class, Text.class, Text.class);
        job.setNumReduceTasks(0);
        info.info(String.format("using config as type:%s workPath:%s inputPath:%s outputPath:%s cfgPath:%s", type,
                workPath, inputPath, outPath, cfgPath));
        job.waitForCompletion(true);
        Counters counters = job.getCounters();
        RowCounts mapCounts = RowCounts.toRowCounts(counters, true);
        info.info("mapper summary:");
        info.info(mapCounts.toString());
    }

}
