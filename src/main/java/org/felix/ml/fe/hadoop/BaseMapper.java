package org.felix.ml.fe.hadoop;

import org.felix.ml.fe.MapperException;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.io.PrintWriter;

import static org.felix.ml.fe.util.Constant.multiOutput;

/**
 *
 * 7
 */
public abstract class BaseMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public static final String LOG_DIR = "log";
    public static final String PATH_PATTERN = "part";
    public static final String LOG_INFO = "mapper_info";
    public static final String LOG_WARN = "mapper_warn";
    public static final String LOG_ERROR = "mapper_error";
    protected RowCounts rowCounts = new RowCounts();

    protected MultipleOutputs<KEYOUT, VALUEOUT> mos;

    boolean firstError = true;

    protected void setup(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        mos = new MultipleOutputs<KEYOUT, VALUEOUT>(context);
    }

    @Override
    protected void map(KEYIN key, VALUEIN value, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        rowCounts.incrementTotal();
        try {
            doMap(key, value, context);
            rowCounts.incrementPass();
        } catch (IOException e) {
            rowCounts.addFailCount(e.getClass().getName());
        } catch (InterruptedException e) {
            rowCounts.addFailCount(e.getClass().getName());
        } catch (MapperException e) {
            if (firstError) {
                StringBuilderWriter writer = new StringBuilderWriter();
                e.printStackTrace(new PrintWriter(writer));
                mos.write(multiOutput, key, new Text(writer.toString()), getLogPath(LOG_ERROR));
            }
            rowCounts.addFailCount(e.getCountName());
            mos.write(multiOutput, key, value, getLogPath(LOG_ERROR));
            firstError = false;
        } catch (Exception e) {
            if (firstError) {
                StringBuilderWriter writer = new StringBuilderWriter();
                e.printStackTrace(new PrintWriter(writer));
                mos.write(multiOutput, key, new Text(writer.toString()), getLogPath(LOG_ERROR));
            }
            rowCounts.addFailCount("unknow_" + e.getClass().getName());
            mos.write(multiOutput, key, value, getLogPath(LOG_ERROR));
            firstError = false;
        }
    }

    protected String getLogPath(String key) {
        return String.format("_%s/_%s-%s", LOG_DIR, key, PATH_PATTERN);
    }

    protected abstract void doMap(KEYIN key, VALUEIN value, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException, MapperException;

    @Override
    protected void cleanup(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        rowCounts.setMapperCounter(context);
        mos.close();
    }
}
