package org.felix.ml.fe.reformat.util;

import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.DataFormatException;
import org.felix.ml.fe.normalize.cnf.CnfModel;
import org.apache.commons.lang.StringUtils;
import org.felix.ml.fe.normalize.util.ConvertUtil;

import java.io.IOException;
import java.util.*;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.split;


/**
 * @author xiangcf
 * @date 20170627 15:12
 * <p>
 * 输入数据的格式：第一列是标签，后面每列都是特征编号、冒号、特征取值，列之间用空格或tab一种分开
 */
public class ReformatUtil {

    /**
     * @param line
     * @param cnfModel
     * @return
     * @throws IOException
     */
    public static String convert(String line, CnfModel cnfModel) throws Exception {
        //String[] log_line = split(line, " ");
        String[] log_line = StringUtils.split(line, " ");
        if (log_line.length == 1) {
            throw new DataFormatException("input data has only one column");
        }
        String score = log_line[0];
        List<String> ret_list = convertLogLine(line, cnfModel);
        String ret_str = join(ret_list, " ");
        return format("%s %s", score, ret_str);
    }

    /**
     * 单特征按照use里面的顺序重新编号,从1开始
     * 个性化特征这里还没想好怎么使用
     */
    public static List<String> convertLogLine(String log_str, CnfModel cnfModel) throws ConfigException {
        List<String> ret_list = new ArrayList<String>();
        Map<String, String> fea_map = new HashMap<String, String>();

        String single_str = cnfModel.getSingle().trim();
        String[] single_arr = StringUtils.split(single_str, ",");
        String[] fea_arr = StringUtils.split(log_str, " ");
        int fea_num = fea_arr.length;
        for (int i = 1; i < fea_num; ++i) {
            String[] pair = split(fea_arr[i].trim(), ":");
            if (pair.length != 2)
                continue;
            fea_map.put(pair[0].trim(), pair[1].trim());
        }
        Map<String, String> nameValue_dic = new HashMap<>();
        for (Map.Entry<String, String> et : fea_map.entrySet()) {
            Map<String, String> temp_map = normalizeMultiValue(et.getKey(), et.getValue(), cnfModel, fea_map);
            nameValue_dic.putAll(temp_map);
        }

        for (int i = 0; i < single_arr.length; ++i) {
            String key = single_arr[i];
            if (nameValue_dic.containsKey(key)) {
                ret_list.add(String.format("%d:%s", i + 1, nameValue_dic.get(single_arr[i])));
            }
        }
        return ret_list;
    }

    /**
     * 特征处理，主要处理个性网特征
     * 当前其实支持同一个特征的多种离散化方式（两种），配置方式类似个性化，用波浪线后缀方式
     * 可以看到下面代码中
     * String feature_id = entry.getKey();
     * String f_number = split(feature_id, "~")[0];
     * if (!f_number.equals(key)) continue;
     *
     * @param key
     * @param value
     * @param cnfModel
     * @param fea_map
     * @return
     * @throws Exception
     */
    private static Map<String, String> normalizeMultiValue(String key, String value, CnfModel
            cnfModel, Map<String, String> fea_map) throws ConfigException {
        Map<String, String> nameValue_dic = new HashMap<String, String>();
        //为什么一个特征要遍历整个配置map
        //因为特征编号和特征id不是一一对应的，一个原始的特征编号，可以对应多个派生特征id
        //比如个性化，个性化的灵活性决定了它可以派生多个特征，如topn偏好值，topn偏好度，匹配偏好度
        Map<String, List<Object>> featureCfgMap = cnfModel.getCnf_dict();
        Map<String, String> match_dict = cnfModel.getPersonalMatch_dict();
        for (Map.Entry<String, List<Object>> entry : featureCfgMap.entrySet()) {
            String feature_id = entry.getKey();
            String f_number = split(feature_id, "~")[0];
            if (!f_number.equals(key)) continue;
            List<Object> one_feature_cfgs = entry.getValue();
            //pertop,permatch 的f_number相同
            String proc_type = (String) one_feature_cfgs.get(0);
            //连续特征才有,离散特征这个域是一个空的list
            Object discret_part = one_feature_cfgs.get(1);
            String defaultvalue = (String) one_feature_cfgs.get(2);
            String ret_value = null;
            switch (proc_type) {
                case "cont_mp1":
                case "cont_mp2":
                case "cate_map":
                    ret_value = value;
                    break;
                case "pertop":
                    ret_value = ConvertUtil.normalizePerTop(value, defaultvalue, feature_id);
                    break;
                case "permatch":
                    ret_value = ConvertUtil.normalizePerMatch(value, defaultvalue, feature_id, fea_map, match_dict);
                    break;
            }
            if (null != ret_value) {
                nameValue_dic.put(feature_id, ret_value);
            }

        }

        return nameValue_dic;
    }

}

