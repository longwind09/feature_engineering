package org.felix.ml.fe.discret.hadoop;

import org.felix.ml.fe.MapperException;
import org.felix.ml.fe.hadoop.BaseMapper;
import org.felix.ml.fe.util.Constant;
import org.felix.ml.fe.util.FeatureTrans;
import org.felix.ml.fe.util.StringDecode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.felix.ml.fe.util.Constant.multiOutput;

/**
 *
 * 2
 *          这个步骤就是对所有特征取值进行全局编号,相当于map阶段按照fea*val为key分发,reduce阶段用一个reduce计数,key相同的算一个
 */
public class DicMapper extends BaseMapper<Object, Text, Text, LongWritable> {
    public static final int MAX_SIZE = 1024 * 1024 * 5;
    private List<String> singleList = new ArrayList<String>();
//    combineList  组合特征列表,里面的组合特征间先后顺序是配置顺序，但是单个组合特征各项顺序是字典序
    private List<List<String>> combineList = new ArrayList<List<String>>();
    // 配置文件里指定的组合特征名,用+号连接多个特征
    private List<String> combineKeys = new ArrayList<String>();

    // 这个map好大
    private Map<String, Long> cacheMap = new TreeMap<String, Long>();
    //总特征数量（离散化前）
    private int featSize;

    /**
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        String single = context.getConfiguration().get("single");
        String combine = context.getConfiguration().get("combine");
        if (!StringUtils.isEmpty(single)) {
            boolean singleWarn = StringDecode.setSingleList(single, ",", singleList);
            if (singleWarn) {
                mos.write(multiOutput, new Text("repeat single feature!"),
                        new Text(context.getConfiguration().get(Constant.CONF_ARGS)), getLogPath(LOG_WARN));
            }
        }
        if (!StringUtils.isEmpty(combine)) {
            boolean combineWarn = StringDecode.setCombineList(combine, ",", "*", combineList, combineKeys);
            if (combineWarn) {
                mos.write(multiOutput, new Text("repeat combine feature!"),
                        new Text(context.getConfiguration().get(Constant.CONF_ARGS)), getLogPath(LOG_WARN));
            }
        }
        featSize = singleList.size() + combineList.size();
        mos.write(multiOutput, new Text("mapper_conf"), new Text(String.format("%s single:%s combine:%s",
                context.getConfiguration().get(Constant.CONF_ARGS), single, combine)), getLogPath(LOG_INFO));
    }

    /**
     * 输入特征离散化后的样本,
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @throws MapperException
     */
    @Override
    protected void doMap(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException, MapperException {
        String line = value.toString();
        //解析一行样本,这行里只有单特征,这些单特征
        Map<String, String> map = StringDecode.str2Map(line, 2);
        //统一单特征和组合特征
        Map<String, String> featsMap = FeatureTrans.trans(map, singleList, combineList, combineKeys);
//        if (featsMap.size() != featSize) {
//            throw new MapperException("Incomplete_feature");
//        }
        //初步编号,特征*取值，组合特征使用单特征值加号组合的方式
        FeatureTrans.merge(cacheMap, featsMap);
        if (cacheMap.size() > MAX_SIZE) {
            flushCache(context);
        }
    }

    /**
     * 输出 特征对应取值以及对应全局编号
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private void flushCache(Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Long> entry : cacheMap.entrySet()) {
            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
        }
        cacheMap.clear();
    }

    @Override
    protected void cleanup(Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        if (cacheMap.size() > 0) {
            flushCache(context);
        }
        super.cleanup(context);
    }
}
