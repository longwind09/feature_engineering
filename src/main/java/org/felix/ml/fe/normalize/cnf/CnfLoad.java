package org.felix.ml.fe.normalize.cnf;

import org.felix.ml.fe.ConfigException;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.apache.commons.lang.StringUtils.*;

/**
 *
 *
 * Transfer from python file!
 * 这是离散化的配置文件的解析，雍坤从python版本的改过来的
 * 雍坤对java语言确实很熟，各种文件处理API信手拈来
 */

/**
 * 现在看来，这块写得烂的原因是，离散化这块没有拆开来成为一个独立的模块，写成蹩脚的role object，恶心死了
 */
public class CnfLoad {
    private static final ArrayList discretTypeList = new ArrayList() {{
        add("cont_mp1");
        add("cate_map");
        //top n top v
        add("pertop");
        //need match_dict,还支持combine
        add("permatch");
        //newly add
        add("cont_mp2");
    }};

    public static CnfModel load(String str) throws IOException, ConfigException {
        String[] arrs = split(str, "\n");
        List<String> list = new ArrayList<String>();
        list.addAll(Arrays.asList(arrs));
        return load(list);
    }

    public static CnfModel load(File cnf) throws IOException, ConfigException {
        return load(new FileInputStream(cnf));
    }

    public static CnfModel load(InputStream in) throws IOException, ConfigException {
        List<String> lines = IOUtils.readLines(in);
        return load(lines);
    }

    /**
     * 特征离散化文件解析
     *
     * @param lines
     * @return
     * @throws IOException
     * @throws ConfigException
     */
    public static CnfModel load(List<String> lines) throws IOException, ConfigException {
        //linkedhashmap 能保证读取的顺序和插入的顺序一致
        Map<String, List<Object>> feature_cfg_dic = new LinkedHashMap<String, List<Object>>();
        Map<String, String> personal_match_dic = new HashMap<String, String>();
        String single = "";
        String merge = "";
        for (String line : lines) {
            //单特征有哪些,组合特征有哪些
            if (line.startsWith("single=")) {
                String v = trim(split(line, "=", 2)[1]);
                single = v;
            } else if (line.startsWith("merge=")) {
                String v = trim(split(line, "=", 2)[1]);
                merge = v;
            }
            //注释以及不完整的行会被滤掉
            if (line.startsWith("#") || !line.contains(":") || !line.contains("="))
                continue;

            //个性化偏好match_dict,个性化特征编号在前，被偏好特征编号在后
            if (line.startsWith("match_dict=")) {
                String v = trim(split(line, "=", 2)[1]);
                JSONObject json = new JSONObject(v);
                Iterator<String> iter = json.keys();
                while (iter.hasNext()) {
                    String key = iter.next();
                    personal_match_dic.put(key, json.getString(key));
                }
                continue;
            }

            //普通行，离散化处理
            String[] one_filed_config = split(line, "=", 2);
            //特征id,因为有派生特征，所以与特征编号有区别
            String feature_id = one_filed_config[0];
            //分段
            String[] discret_parts = split(one_filed_config[1], ":");
            String discret_type = discret_parts[0];
            if (!discretTypeList.contains(discret_type)) continue;

            //用 Object??
            List<Object> one_feature_config_in_a_list = new ArrayList<Object>();

            //cate_map对应的ret_list都为空
            //个性化当前使用方式都是cate_map
            Object ret_list = new ArrayList<String>();
            String default_val = "-1";
            switch (discret_type) {
                // 连续值等区间离散化
                case "cont_mp1":
                    ret_list = LoadFixCounFieldCfg(discret_parts[1]);
                    default_val = "" + Float.parseFloat(discret_parts[2]);
                    break;
                //类型值
                case "cate_map":
                    default_val = "" + Float.parseFloat(discret_parts[1]);
                    break;
                //个性化取最偏好
                case "pertop":
                    default_val = "" + Integer.parseInt(discret_parts[1]);
                    break;
                //个性化匹配度
                case "permatch":
                    default_val = "" + Float.parseFloat(discret_parts[1]);
                    break;
                //自定义离散化连续值
                case "cont_mp2":
                    ret_list = LoadCustomCounFieldCfg(discret_parts[1]);
                    default_val = "" + Float.parseFloat(discret_parts[2]);
                    break;
            }

            //离散化方法
            one_feature_config_in_a_list.add(discret_type);
            //离散化三元组
            one_feature_config_in_a_list.add(ret_list);
            //
            one_feature_config_in_a_list.add(default_val);
            feature_cfg_dic.put(feature_id, one_feature_config_in_a_list);
        }
        //single, merge 不能同时为空
        if (feature_cfg_dic.size() == 0 || (isEmpty(single) && isEmpty(merge)))
            throw new ConfigException();
        return new CnfModel(feature_cfg_dic, personal_match_dic, single, merge);
    }

    public static float[] LoadCustomCounFieldCfg(String str) throws ConfigException {
        String[] arry = split(str, ",");
        int size = arry.length;
        float[] ret = new float[size * 2];
        for (int i = 0; i < size; ++i) {
            String[] t_arr = split(arry[i], "~");
            if (t_arr.length != 2) {
                System.err.println("cont field config wrong!!!");
                throw new ConfigException();
            }
            try {
                ret[2 * i] = Float.parseFloat(t_arr[0]);
                ret[2 * i + 1] = Float.parseFloat(t_arr[1]);
            } catch (NumberFormatException e) {
                System.err.println("numberformat exception");
                throw new ConfigException();
            }
        }
        return ret;
    }

    /**
     * 连续值定长区间
     * 3个浮点数，分别是start，step，max_step
     *
     * @param str
     * @return
     */
    public static float[] LoadFixCounFieldCfg(String str) throws ConfigException {
        String[] arry = split(str, ",");
        if (arry.length == 3) {
            float[] ret = new float[3];
            for (int i = 0; i < 3; i++) {
                ret[i] = Float.parseFloat(arry[i]);
            }
            return ret;
        } else {
            System.err.println("cont field config wrong!!!");
            throw new ConfigException();
        }
    }

}
