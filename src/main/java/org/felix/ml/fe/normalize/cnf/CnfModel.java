package org.felix.ml.fe.normalize.cnf;

import java.util.List;
import java.util.Map;

/**
 *
 *
 *          <p>
 *          离散化配置文件的类
 */
public class CnfModel {
    private Map<String, List<Object>> cnf_dict;
    private Map<String, String> match_dict;
    private String single;
    private String merge;

    public CnfModel(Map<String, List<Object>> cnf_dict, Map<String, String> match_dict, String single, String merge) {
        this.cnf_dict = cnf_dict;
        this.match_dict = match_dict;
        this.single = single;
        this.merge = merge;
    }

    public Map<String, List<Object>> getCnf_dict() {
        return cnf_dict;
    }

    public Map<String, String> getPersonalMatch_dict() {
        return match_dict;
    }

    public String getSingle() {
        return single;
    }

    public String getMerge() {
        return merge;
    }
}
