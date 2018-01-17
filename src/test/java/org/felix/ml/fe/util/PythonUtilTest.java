package org.felix.ml.fe.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;


/**
 *
 *
 */
@RunWith(JUnit4.class)
public class PythonUtilTest {
    @Test
    public void test() {
        Assert.assertEquals("1.0", PythonUtil.format(1.0000d, 4));
        Assert.assertEquals("0.5", PythonUtil.format(0.500d, 4));
        Assert.assertEquals("0.4449", PythonUtil.format(0.44485d, 4));
        Assert.assertEquals("0.4448", PythonUtil.format(0.44481d, 4));
        Assert.assertEquals("0", PythonUtil.format(0d, 4));
    }
}
