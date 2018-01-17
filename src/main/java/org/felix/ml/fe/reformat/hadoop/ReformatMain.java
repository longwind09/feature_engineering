package org.felix.ml.fe.reformat.hadoop;


import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.hadoop.RowCounts;
import org.felix.ml.fe.normalize.cnf.CnfLoad;
import org.felix.ml.fe.util.HadoopUtil;
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
import org.felix.ml.fe.util.Constant;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 *
 * <p>
 * 这个类主要是做特征离散化的
 * 连续特征--分段离散化
 * 类别特征--按实际取值种类离散化
 */

/**
 *  2017/6/27.
 */
public class ReformatMain {
    public static final String cnfFile = "cfg.txt";
    private static Logger info = Logger.getLogger("info");
    private static Logger warn = Logger.getLogger("warn");

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException, ConfigException {

        //hadoop part
        Configuration conf = new Configuration();
        //hadoop 获取参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        if (otherArgs.length <= 1) {
            warn.error("argv error");
            warn.error("eg:workPath jobname=xxx&type=TRAIN|TEST&indir=&outdir=&cfg=);");
            System.exit(2);
        }
        //必填参数--工作路径
        String workPath = otherArgs[0];
        //必填参数--其他参数
        String config = otherArgs[1];
        //解析可选参数
        HadoopUtil.initConf(conf, config, null);

        //可选参数，type:TRAIN,TEST,默认是TRAIN
        String type = conf.get("type", Constant.DEFAULT_TYPE_TRAIN);
        //可选参数，输入路径,默认是抽样后的路径
        String inputPath = Constant.getPath(workPath, type, conf.get("indir", Constant.DEFAULT_FILTER_DIR));
        //可选参数，输出路径，默认是离散化路径
        String outPath = Constant.getPath(workPath, type, conf.get("outdir", Constant.DEFAULT_REFORMAT_DIR));
        //可选参数，离散化配置文件名，默认是cfg.txt
        String cfgPath = Constant.getCnfPath(workPath, conf.get("cfg", Constant.DEFAULT_CNF));
        //必填参数 jobname
        String jobName = conf.get("jobname");

        //hdfs 文件读取案例
        conf.set("mapred.create.symlink", "yes");
        DistributedCache.createSymlink(conf);

        //读取离散化配置文件
        Path filePath = new Path(cfgPath);
        FileSystem fs = FileSystem.get(URI.create(cfgPath), conf);
        InputStream in = null;
        in = fs.open(new Path(cfgPath));
        CnfLoad.load(in);


        //把当前配置文件创建符号链接,加到分布式缓存
        String uriWithLink = filePath.toUri().toString() + "#" + ReformatMain.cnfFile;
        DistributedCache.addCacheFile(new URI(uriWithLink), conf);


        //配置hadoopjob
        Job job = new Job(conf, jobName);
        HadoopUtil.setInput(conf, job, inputPath);
        job.setJarByClass(ReformatMain.class);
        job.setMapperClass(ReformatMapper.class);
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
