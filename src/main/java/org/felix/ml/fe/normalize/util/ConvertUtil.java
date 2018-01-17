package org.felix.ml.fe.normalize.util;

import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.normalize.cnf.CnfModel;
import org.apache.commons.lang.StringUtils;

import java.util.*;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.*;


/**
 * xiangcf 根据房产商业算法组同学们对个性化特征的使用，对原来质量排序组同学的离散化流程做了一些修改：
 * 雍坤之前使用的是搜索排序同学的老版（python版）改过来的，基本没有做任何逻辑改动，所以也存在一些问题
 * <p>
 * <p>
 * 当前这个类效率极低，比如多个个性化特征其实可以只解析一次个性化串，实际上执行了多次
 */
public class ConvertUtil {
    public static final int INT_NAN = -9999;
    public static final String DISCRET_NAN = "-1";
    public static final String NO_INTEREST = "0";
    private static final String NO_COMBINE = "0";
    private static final String DO_COMBINE = "1";
    private static final String GAP_HUGE = "9999";

    /**
     * @param line
     * @param cnfModel
     * @return
     */
    public static String convert(String line, CnfModel cnfModel) {
        String[] log_fields = split(line, " ");
        String score = log_fields[0];
        try {

            List<String> ret_list = parseFields(log_fields, cnfModel);
            String ret_str = join(ret_list, " ");
            return format("%s %s", score, ret_str);
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 离散化过程就是一次再编码过程，把输入值映射一下
     *
     * @param logFields 从index 1开始，index0 是score
     * @param cnfModel  特征选择配置文件
     * @return
     */
    public static List<String> parseFields(String[] logFields, CnfModel cnfModel) throws Exception {
        List<String> ret_list = new ArrayList<String>();
        Map<String, String> one_line_str2map = new HashMap<String, String>();
        //str2map
        for (int i = 1; i < logFields.length; ++i) {
            String[] val_ary = split(logFields[i], ":");
            one_line_str2map.put(val_ary[0], val_ary[1]);
        }
        for (int i = 1; i < logFields.length; ++i) {
            String[] val_ary = split(logFields[i], ":");
            //
            List<Map<String, String>> nameValue_dic_list = normalizeMultiValue(val_ary[0], val_ary[1],
                    cnfModel.getCnf_dict(), cnfModel.getPersonalMatch_dict(), one_line_str2map);
            //map2str
            for (Map<String, String> d : nameValue_dic_list) {
                for (Map.Entry<String, String> entry : d.entrySet()) {
                    ret_list.add(String.format("%s:%s", entry.getKey(), entry.getValue()));
                }
            }
        }
        return ret_list;
    }


    /**
     * java 没有元组所以map被滥用了
     *
     * @param feature_num      特征编号,一个原始特征结果特征处理有可能变成多个特征
     * @param val_str
     * @param featureCfgMap
     * @param match_dict
     * @param one_line_str2map
     * @return 多个特征与取值的列表
     */
    public static List<Map<String, String>> normalizeMultiValue(String feature_num, String val_str,
                                                                Map<String, List<Object>> featureCfgMap, Map<String, String> match_dict, Map<String, String> one_line_str2map) throws Exception {
        List<Map<String, String>> nameValue_dic_list = new ArrayList<Map<String, String>>();

        //为什么一个特征要遍历整个配置map
        //因为特征编号和特征id不是一一对应的，一个原始的特征编号，可以对应多个派生特征id
        //比如个性化，个性化的灵活性决定了它可以派生多个特征，如topn偏好值，topn偏好度，匹配偏好度
        for (Map.Entry<String, List<Object>> entry : featureCfgMap.entrySet()) {

            String feature_id = entry.getKey();
            String f_number = split(feature_id, "~")[0];
            if (!f_number.equals(feature_num)) continue;

            Map<String, String> nameValue_dic = new LinkedHashMap<>();
            List<Object> one_feature_cfgs = entry.getValue();
            //pertop,permatch 的f_number相同
            String proc_type = (String) one_feature_cfgs.get(0);
            Object discret_part = one_feature_cfgs.get(1);
            String defaultvalue = (String) one_feature_cfgs.get(2);
            String discret_value = null;
            switch (proc_type) {
                case "cont_mp1":
                    discret_value = normalizeFixCouField(val_str, discret_part, defaultvalue);
                    break;

                case "cont_mp2":
                    discret_value = normalizeCustomCouField(val_str, discret_part, defaultvalue);
                    break;

                case "cate_map":
                    discret_value = discretCateField(val_str, defaultvalue);
                    break;
                case "pertop":
                    discret_value = normalizePerTop(val_str, defaultvalue, feature_id);
                    break;
                case "permatch":
                    discret_value = normalizePerMatch(val_str, defaultvalue, feature_id, one_line_str2map, match_dict);
                    break;
            }
            if (null != discret_value) {
                nameValue_dic.put(feature_id, discret_value);
                nameValue_dic_list.add(nameValue_dic);
            }

        }

        return nameValue_dic_list;
    }

    private static String discretCateField(String val_str, String defaultvalue) {
        long val_long = INT_NAN;
        try {
            val_long = (long) Float.parseFloat(val_str);
        } catch (Exception e) {
        }
        if (val_long == INT_NAN)
            return defaultvalue;
        return val_str;
    }

    /**
     * 连续值的离散化
     * 这里因为精度问题，可能离散化得到的值不一样,建议统一精度
     *
     * @param val_str
     * @param discret_parts
     * @param defaultvale
     * @return
     */
    public static String normalizeFixCouField(String val_str, Object discret_parts, String defaultvale) {
        long val_long = INT_NAN;
        try {
            val_long = (long) Float.parseFloat(val_str);
        } catch (Exception e) {
        }
        if (val_long == INT_NAN)
            return defaultvale;
        //-----------------------------------------------
        double real_val = Double.parseDouble(val_str);
        float[] roles = (float[]) discret_parts;
//        for (double ele : roles) {
//            System.out.println(ele);
//        }
        double cont_start = roles[0];
        double step = roles[1];
        double cont_end = roles[2];
        double proc_val = (real_val - cont_start) / step + 1;
        if (proc_val > cont_end) proc_val = cont_end;
        return String.format("%d", (long) proc_val);
    }

    public static String normalizeCustomCouField(String val_str, Object discret_parts, String defaultvale) throws Exception {

        try {

            double real_val = Double.parseDouble(val_str);
            float[] roles = (float[]) discret_parts;
            for (int i = 0; i < roles.length; i += 2) {
                float lowwer = roles[i];
                float upper = roles[i + 1];
                if (real_val >= lowwer && real_val < upper) {
                    return String.format("%d", (i / 2));
                }
            }
        } catch (Exception e) {
            System.err.println("normalizedCustomConField exception");
            throw new Exception("exception in normalizedCustomConField");
        }
        return defaultvale;
    }


    /**
     * 个性化top n
     * <p>
     * 雍坤这里比较的时候默认第二项是0~1之间的小数，所以没有使用数字比较而是直接使用字符串比较
     *
     * @param val_str      个性化串
     * @param defaultvalue 默认值
     * @param feature_id   当前特征编号,同一个特征的不同的个性化方式对应不同的特征编号
     * @return 个性化特征返回值
     */
    public static String normalizePerTop(String val_str, String defaultvalue, Object feature_id) {
        if (!StringUtils.contains(val_str, "~"))
            return defaultvalue;

//            131:3~0.2529#2~0.6437#1~0.1034
        String[] value_wave_rate_array = split(val_str, "#");
        Map<String, String> origin_value_map = new HashMap<String, String>();

        // 自定义按照key排序比较方便，按照value排序比较麻烦
        Map<String, String> preference_rate_map = new TreeMap<String, String>(new Comparator<String>() {
            //按照偏好度排序,然后实际上不用排序，因为每次只取一次，所以一次选指定大小的值即可
            //用类似快排的思想做交换即可
            public int compare(String o1, String o2) {
                String[] arr1 = split(o1, "~");
                String[] arr2 = split(o2, "~");
//                int ret = arr2[1].compareTo(arr1[1]);
//                if (ret != 0)
//                    return ret;
//                return arr2[0].compareTo(arr1[0]);


                double t2 = Double.parseDouble(arr2[1]);
                double t1 = Double.parseDouble(arr1[1]);
                if (t2 != t1) {
                    return t2 < t1 ? -1 : 1;
                }
                return arr2[0].compareTo(arr1[0]);
            }
        });

        //放到map里
        for (String fea : value_wave_rate_array) {
            if (!StringUtils.contains(fea, "~"))
                continue;
//            3~0.2529
            String[] fea_val = split(fea, "~");
            preference_rate_map.put(fea, fea_val[1]);
            origin_value_map.put(fea, fea_val[0]);
        }


        // 143~1~n
        String[] feature_id_parts = split(trim((String) feature_id), "~");
        String f_number = feature_id_parts[0];
        String topn = feature_id_parts[1];
        String valueType = feature_id_parts[2];
        int num_topn = Integer.parseInt(topn);
        String index_key = null;
        switch (valueType) {
            case "n":
                index_key = preference_rate_map.keySet().toArray()[num_topn - 1].toString();
                return origin_value_map.get(index_key);
            case "v":
                index_key = preference_rate_map.keySet().toArray()[num_topn - 1].toString();
                return preference_rate_map.get(index_key);
        }

        return defaultvalue;
    }

    /**
     * permatch 的feature_id 就是它的特征编号
     *  个性化的配置异常不应该在实际特征工程的时候检查，而应该再特征工程配置文件加载和解析时检测
     * @param val_str
     * @param defaultvalue
     * @param feature_id      特征名称
     * @param oneline_fea_map
     * @param match_dict
     * @return 偏好度
     */
    public static String normalizePerMatch(String val_str, String defaultvalue, String feature_id,
                                           Map<String, String> oneline_fea_map, Map<String, String> match_dict) throws ConfigException {
        if (!StringUtils.contains(val_str, "~"))
            return defaultvalue;
        String[] value_wave_rate_arr = split(val_str, "#");
        Map<String, String> value_rate_map = new HashMap<String, String>();
        List<String> li = new ArrayList<String>();

        for (String fea : value_wave_rate_arr) {
            if (!StringUtils.contains(fea, "~"))
                continue;
            String[] fea_val = split(fea, "~", 2);
            value_rate_map.put(fea_val[0].trim(), fea_val[1].trim());
            li.add(fea_val[1].trim());
        }

        String[] feature_id_parts = split(trim((String) feature_id), "~");
        String person_ori_name = feature_id_parts[0];
        String matched_origin_field = match_dict.get(person_ori_name);
        String match_value = NO_INTEREST;

        String feature_value = oneline_fea_map.get(matched_origin_field);
        if (value_rate_map.containsKey(feature_value))
            match_value = value_rate_map.get(feature_value);
        if (feature_id_parts.length != 3) {
            return match_value;
        }

        String topn = feature_id_parts[1];
        String valueType = feature_id_parts[2];
        int num_topn = Integer.parseInt(topn);
        int kth_value = get_nth(li.toArray(new String[li.size()]), num_topn);
        switch (valueType) {
            case "c":
                if (match_value.equals(NO_INTEREST)) {
                    return NO_COMBINE;
                }
                if (String.valueOf(kth_value).equals(match_value)) {
                    return DO_COMBINE;
                }
                return NO_COMBINE;
            case "g":
                if (match_value.equals(NO_INTEREST)) {
                    return String.valueOf(kth_value);
                }
                int num_match_value = Integer.parseInt(match_value);
                return String.valueOf(kth_value - num_match_value);
            default:
                throw new ConfigException("nonsupported personal value type");
        }


    }

    private static int get_nth(String[] li, int k) {
        if (k > li.length) {
            k = li.length;
            System.out.println("certain user does not have k" +
                    "different favors on certain feature");
        }
        int[] arr = new int[li.length];
        for (int i = 0; i < arr.length; ++i) {
            arr[i] = Integer.parseInt(li[i]);
        }
        return search(arr, 0, arr.length - 1, k);
    }

    static int partition(int[] arr, int low, int high) {
        int x = arr[low];
        while (low < high) {
            while (low < high && arr[high] <= x) --high;
            arr[low] = arr[high];
            while (low < high && arr[low] >= x) ++low;
            arr[high] = arr[low];
        }
        arr[low] = x;
        return low;
    }

    static int search(int[] arr, int i, int j, int k) {
        assert (i <= j);
        int q = partition(arr, i, j);
        if (q - i + 1 == k) {
            return arr[q];
        } else if (q - i + 1 < k) {
            return search(arr, q + 1, j, k - (q - i + 1));
        } else {
            return search(arr, i, q - 1, k);
        }
    }


}
