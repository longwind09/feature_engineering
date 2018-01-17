package org.felix.ml.fe.util;

/**
 *
 *
 */
public class Constant {
    public static final String multiOutput = "multioutput";
    //public static final String QID = "qid";
    public static final String QID = "SSqid";
    public static final String UID = "uid";
    public static final String CONF_ARGS = "_args";

    public static final String DEFAULT_FILTER_DIR = "filter_job";
    public static final String DEFAULT_NORM_DIR = "normalize_job";
    public static final String DEFAULT_KEY_DIR = "keyalloc_job";
    public static final String DEFAULT_DISCRET_DIR = "discret_job";
    public static final String DEFAULT_REFORMAT_DIR = "reformat_job";
    public static final String DEFAULT_CNF = "cfg.txt";
    public static final String DEFAULT_TYPE_TRAIN = "TRAIN";
    public static final String DEFAULT_TYPE_TEST = "TEST";
    public static final String DEFAULT_PART = "part-r-00000";
    public static final String DEFAULT_PART_2 = "part-r-00001";

    /**
     * 获取全路径
     *
     * @param work
     * @param type
     * @param dir
     * @return
     */
    public static String getPath(String work, String type, String dir) {
        return String.format("%s/%s/%s", work, type, dir);
    }

    /**
     * 获取配置文件路径
     *
     * @param work
     * @param cfg
     * @return
     */
    public static String getCnfPath(String work, String cfg) {
        return String.format("%s/%s", work, cfg);
    }

    /**
     * 获取字典
     *
     * @param work
     * @param part
     * @return
     */
    public static String getDicPath(String work, String part) {
        return String.format("%s/%s/%s", work, DEFAULT_KEY_DIR, part);
    }

    public static String getDicPath(String work, String part, String type) {
        return String.format("%s/%s/%s/%s", work, type, DEFAULT_KEY_DIR, part);
    }
}
