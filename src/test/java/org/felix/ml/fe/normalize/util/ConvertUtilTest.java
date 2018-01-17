package org.felix.ml.fe.normalize.util;

import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.normalize.cnf.CnfLoad;
import org.felix.ml.fe.normalize.cnf.CnfModel;
import org.json.JSONException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 *
 */
@RunWith(JUnit4.class)
public class ConvertUtilTest {
    private String other = "\nsingle=101\n";

    @Test
    public void test_cont_mp1() throws IOException, ConfigException {
        String str = "pos=cont_mp1:-1,1,2000:-1:2010\n"
                + "105=cont_mp1:-1,1,5:-1:15" + other;
        List<String[]> testCase = new ArrayList<String[]>() {{
            add(new String[]{"sort_id pos:23", "sort_id pos:25"});
            add(new String[]{"sort_id 105:-9999", "sort_id 105:-1.0"});
        }};
        CnfModel cnfModel = CnfLoad.load(str);
        for (String[] acase : testCase) {
            String real = ConvertUtil.convert(acase[0], cnfModel);
            assertEquals(acase[1], real);
        }
    }

    @Test
    public void test_cont_mp1_2() throws IOException, ConfigException {
        long c = 106751974085l;
        System.out.println("" + c);
        String str = "131=cont_mp1:-1,1440,1000000:-2:1000010" + other;
        List<String[]> testCase = new ArrayList<String[]>() {{
            add(new String[]{"sort_id 131:-153722842682445", "sort_id 131:-106751974084"});
        }};
        CnfModel cnfModel = CnfLoad.load(str);
        for (String[] acase : testCase) {
            String real = ConvertUtil.convert(acase[0], cnfModel);
            assertEquals(acase[1], real);
        }
    }

    @Test
    public void test_cate_map() throws IOException, ConfigException {
        String str = "106=cate_map:-1:20\n" + other;
        List<String[]> testCase = new ArrayList<String[]>() {{
            add(new String[]{"sort_id 106:-9999", "sort_id 106:-1.0"});
        }};
        CnfModel cnfModel = CnfLoad.load(str);
        for (String[] acase : testCase) {
            String real = ConvertUtil.convert(acase[0], cnfModel);
            assertEquals(acase[1], real);
        }
    }

    @Test
    public void test_pertop() throws IOException, JSONException, ConfigException {
        String str = "601~1~n=pertop:-9999:20\n"
                + "601~1~v=pertop:-9999:20" + other;
        List<String[]> testCase = new ArrayList<String[]>() {{
            add(new String[]{"sort_id 601:1~0.0097#2~0.2754#3~0.6522#4~0.0531#5~0.0097", "sort_id 601~1~n:3 601~1~v:0.6522"});
        }};
        CnfModel cnfModel = CnfLoad.load(str);
        for (String[] acase : testCase) {
            String real = ConvertUtil.convert(acase[0], cnfModel);
            assertEquals(acase[1], real);
        }
    }

    @Test
    public void test_pertop2() throws IOException, JSONException, ConfigException {
        String str = "601~1~n=pertop:-9999:20" + other;
        List<String[]> testCase = new ArrayList<String[]>() {{
            add(new String[]{"sort_id 601:-9999", "sort_id 601~1~n:-9999"});
        }};
        CnfModel cnfModel = CnfLoad.load(str);
        for (String[] acase : testCase) {
            String real = ConvertUtil.convert(acase[0], cnfModel);
            assertEquals(acase[1], real);
        }
    }

    @Test
    public void test_permatch() throws IOException, JSONException, ConfigException {
        String str = "match_dict={\"601\":\"103\"}\n"
                + "601=permatch:-1:20" + other;
        List<String[]> testCase = new ArrayList<String[]>() {{
            add(new String[]{"sort_id 103:8 601:-9999~0.058#1~0.0097#10~0.0048#2~0.029#4~0.0048#5~0.058#6~0.7971#7~0.0193#8~0.0193", "sort_id 601:0.0193"});
        }};
        CnfModel cnfModel = CnfLoad.load(str);
        for (String[] acase : testCase) {
            String real = ConvertUtil.convert(acase[0], cnfModel);
            assertEquals(acase[1], real);
        }
    }
}
