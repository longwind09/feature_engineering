package org.felix.ml.fe.discret.service.impl;

import org.felix.ml.fe.discret.service.IFeatureIndexService;
import org.felix.ml.fe.discret.service.UnsupportException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

/**
 *
 * 9
 */
@RunWith(JUnit4.class)
public class ListFeatureIndexServiceImplTest {
    public static final String dicFile = "dic.txt";
    IFeatureIndexService featureIndexService;

    @Before
    public void before() throws IOException, UnsupportException, URISyntaxException {
        featureIndexService = new ListFeatureIndexServiceImpl();
        URL url = ListFeatureIndexServiceImplTest.class.getResource(dicFile);
        featureIndexService.init(new File(url.toURI()));
    }

    @After
    public void after() {
        featureIndexService = null;
    }

    @Test
    public void find() throws IOException {
        List<String> ret = IOUtils.readLines(ListFeatureIndexServiceImplTest.class.getResourceAsStream(dicFile));
        for (String key : ret) {
            String[] arrs = StringUtils.split(key, "\t");
            int index = featureIndexService.search(arrs[0]);
            Assert.assertTrue(index >= 0);
            String find = featureIndexService.get(index);
            Assert.assertEquals(find, arrs[0]);
        }
        String unfind = "unfind";
        int index = featureIndexService.search(unfind);
        Assert.assertTrue(index < 0);
    }
}
