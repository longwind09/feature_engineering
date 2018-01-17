package org.felix.ml.fe.discret.hadoop;

import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.hadoop.RowCounts;
import org.felix.ml.fe.normalize.cnf.CnfLoad;
import org.felix.ml.fe.normalize.cnf.CnfModel;
import org.felix.ml.fe.util.HadoopUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

import static org.felix.ml.fe.util.Constant.*;

/**
 *
 *
 */
public class DicMain {
    private static Logger info = Logger.getLogger("info");
    private static Logger warn = Logger.getLogger("warn");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ConfigException {
        Configuration conf = new Configuration();

        //----------------- resolve args
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length <= 1) {
            warn.error("argv error");
            warn.error("eg:workPath jobname=xxx&keytype=xxx&indir=&outdir=);");
            System.exit(2);
        }
        String workPath = otherArgs[0];
        String config = otherArgs[1];
        HadoopUtil.initConf(conf, config, null);
        String type = conf.get("type", DEFAULT_TYPE_TRAIN);
        String inputPath = getPath(workPath, type, conf.get("indir", DEFAULT_NORM_DIR));
        String outPath = getPath(workPath, type, conf.get("outdir", DEFAULT_KEY_DIR));
        String cfgPath = getCnfPath(workPath, conf.get("cfg", DEFAULT_CNF));
        //-------------------------


        //------------------------resolve feature.cfg
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
        //----------------------------


        //---------- hadoop job
        String jobName = conf.get("jobname");
        Job job = new Job(conf, jobName);
        HadoopUtil.setInput(conf, job, inputPath);
        job.setJarByClass(DicMain.class);
        job.setMapperClass(DicMapper.class);
        job.setReducerClass(DicReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        MultipleOutputs.addNamedOutput(job, multiOutput, TextOutputFormat.class, Text.class, Text.class);
        job.setNumReduceTasks(1);
        info.info(String.format("using config as type:%s workPath:%s inputPath:%s outputPath:%s cfgPath:%s", type,
                workPath, inputPath, outPath, cfgPath));
        job.waitForCompletion(true);
        //------------------------------


        //----------------- statistics about the job
        Counters counters = job.getCounters();
        RowCounts mapCounts = RowCounts.toRowCounts(counters, true);
        RowCounts reducerCounts = RowCounts.toRowCounts(counters, false);
        info.info("mapper summary:");
        info.info(mapCounts.toString());
        info.info("reducer summary:");
        info.info(reducerCounts.toString());
        //------------------------------------
    }

}
