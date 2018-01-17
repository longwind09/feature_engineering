package org.felix.ml.fe.discret.hadoop;

import org.felix.ml.fe.MapperException;
import org.felix.ml.fe.discret.service.FeatureIndexServiceFactory;
import org.felix.ml.fe.discret.service.IFeatureIndexService;
import org.felix.ml.fe.discret.service.UnsupportException;
import org.felix.ml.fe.hadoop.BaseMapper;
import org.felix.ml.fe.util.Constant;
import org.felix.ml.fe.util.FeatureTrans;
import org.felix.ml.fe.util.StringDecode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 *
 *
 * updated by xiangcf @20171126
 * 支持不完整的特征，即稀疏特征
 */
public class DiscretMapper extends BaseMapper<Object, Text, Text, Text> {
    public static final String dicFile = "dic";
    private List<String> singleList = new ArrayList<String>();
    private List<List<String>> combineList = new ArrayList<List<String>>();
    private List<String> combineKeys = new ArrayList<String>();
    private int featSize;
    private IFeatureIndexService featureIndexService;

    /**
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        String single = context.getConfiguration().get("single");
        String combine = context.getConfiguration().get("combine");
        //解析单特征
        if (!StringUtils.isEmpty(single)) {
            //特征重复了不影响模型训练流程，只是会警告
            boolean singleWarn = StringDecode.setSingleList(single, ",", singleList);
            if (singleWarn) {
                mos.write(Constant.multiOutput, new Text("repeat single feature!"),
                        new Text(context.getConfiguration().get(Constant.CONF_ARGS)), getLogPath(LOG_WARN));
            }
        }

        //解析组合特征
        if (!StringUtils.isEmpty(combine)) {
            //组合特征重复
            boolean combineWarn = StringDecode.setCombineList(combine, ",", "*", combineList, combineKeys);
            if (combineWarn) {
                mos.write(Constant.multiOutput, new Text("repeat combine feature!"),
                        new Text(context.getConfiguration().get(Constant.CONF_ARGS)), getLogPath(LOG_WARN));
            }
        }
        featSize = singleList.size() + combineList.size();
        File dic = new File(dicFile);
        try {
            //
            featureIndexService = FeatureIndexServiceFactory.getFeatureIndexService(dic);
        } catch (UnsupportException e) {
            throw new InterruptedException();
        }
        mos.write(Constant.multiOutput, new Text("mapper_conf"),
                new Text(String.format("%s single:%s combine:%s dic_size:%s featSize:%s service:%s",
                        context.getConfiguration().get(Constant.CONF_ARGS), single, combine, featureIndexService.size(),
                        featSize, featureIndexService.getClass().getSimpleName())),
                getLogPath(LOG_INFO));
    }

    /**
     * 利用离散化文件和穷举文件dict-part来做one-hot编码，难点在于全局编号容易超内存
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @throws MapperException
     */
    @Override
    protected void doMap(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException, MapperException {
        String line = value.toString();
        Map<String, String> map = StringDecode.str2Map(line, 2);
        Map<String, String> featsMap = FeatureTrans.trans(map, singleList, combineList, combineKeys);

        //
//        if (featsMap.size() != featSize) {
//            throw new MapperException("Incomplete_feature");
//        }
        String[] arrs = StringUtils.split(line, " ", 2);
        List<String> feats = new ArrayList<String>();
        for (Map.Entry<String, String> entry : featsMap.entrySet()) {
            String kv = FeatureTrans.getKVString(entry.getKey(), entry.getValue());

            //如果没有找到，则warn
            int index = featureIndexService.search(kv);
            if (index < 0) {
                rowCounts.addWranCount(entry.getKey());
                continue;
            }
            //one-hot 编码
            //!!!注意了，编号从1开始
            feats.add(String.format("%s:%s", index + 1, 1));
        }
        if (feats.size() == 0)
            throw new MapperException("empty_feature");
        context.write(new Text(String.format("%s %s", arrs[0], StringUtils.join(feats, " "))), new Text(""));
    }
}
