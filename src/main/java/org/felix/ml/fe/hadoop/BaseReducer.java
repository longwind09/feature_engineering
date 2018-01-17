package org.felix.ml.fe.hadoop;

import org.felix.ml.fe.ReducerException;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.io.PrintWriter;

import static org.felix.ml.fe.util.Constant.multiOutput;

/**
 *
 * 8
 */
public abstract class BaseReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public static final String LOG_DIR = "log";
    public static final String PATH_PATTERN = "part";
    public static final String LOG_INFO = "reducer_info";
    public static final String LOG_WARN = "reducer_warn";
    public static final String LOG_ERROR = "reducer_error";
    protected MultipleOutputs<KEYOUT, VALUEOUT> mos;
    protected RowCounts rowCounts = new RowCounts();
    boolean firstError = true;

    @Override
    protected void setup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<KEYOUT, VALUEOUT>(context);
    }

    @Override
    protected void cleanup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        rowCounts.setReducerCounter(context);
        mos.close();
    }

    @Override
    protected void reduce(KEYIN key, Iterable<VALUEIN> value, Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        rowCounts.incrementTotal();
        try {
            doReduce(key, value, context);
            rowCounts.incrementPass();
        } catch (IOException e) {
            rowCounts.addFailCount(e.getClass().getName());
        } catch (InterruptedException e) {
            rowCounts.addFailCount(e.getClass().getName());
        } catch (ReducerException e) {
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

    protected abstract void doReduce(KEYIN arg0, Iterable<VALUEIN> arg1,
                                     Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context arg2)
            throws IOException, InterruptedException, ReducerException;
}
