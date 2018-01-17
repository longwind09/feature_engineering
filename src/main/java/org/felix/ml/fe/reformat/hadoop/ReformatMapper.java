package org.felix.ml.fe.reformat.hadoop;

import org.felix.ml.fe.ConfigException;
import org.felix.ml.fe.MapperException;
import org.felix.ml.fe.hadoop.BaseMapper;
import org.felix.ml.fe.normalize.cnf.CnfLoad;
import org.felix.ml.fe.normalize.cnf.CnfModel;
import org.felix.ml.fe.reformat.util.ReformatUtil;
import org.felix.ml.fe.util.Constant;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.felix.ml.fe.util.Constant.multiOutput;

/**
 *
 *
 *          <p>
 *          这个步骤的作用是，把连续特征离散化，做了一次值的转化
 *          <p>
 *          我不太清楚的地方是个性化偏好特征如何
 */
public class ReformatMapper extends BaseMapper<Object, Text, Text, Text> {
    public static final String cnfFile = "cfg.txt";
    CnfModel cnfModel;
    List<String> singleFeatureList = null;
    boolean initSuccess = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos.write(multiOutput, new Text("mapper_conf"), new Text(context.getConfiguration().get(Constant.CONF_ARGS)),
                getLogPath(LOG_INFO));
        try {
            loadCnfFile();
        } catch (ConfigException e) {
            initSuccess = false;
        }
    }

    private void loadCnfFile() throws IOException, ConfigException {
        cnfModel = CnfLoad.load(new File(cnfFile));
        String[] singleFeatureArray = cnfModel.getSingle().split(",");
        singleFeatureList = Arrays.asList(singleFeatureArray);
    }

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @throws MapperException
     */
    @Override
    protected void doMap(Object key, Text value, Context context)
            throws IOException, InterruptedException, MapperException {
        if (!initSuccess)
            throw new MapperException("initFail!");
        String line = value.toString().trim();
        try {
            String ret = ReformatUtil.convert(line, cnfModel);
            context.write(new Text(ret), null);
        } catch (Exception e) {
            System.err.println("");
            return;
        }
    }

}
