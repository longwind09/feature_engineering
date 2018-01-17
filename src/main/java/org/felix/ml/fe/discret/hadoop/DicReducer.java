package org.felix.ml.fe.discret.hadoop;

import org.felix.ml.fe.ReducerException;
import org.felix.ml.fe.hadoop.BaseReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 * 3
 */
public class DicReducer extends BaseReducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    /**
     * 实际上这里输出的才是真正的每个特征每个取值的个数
     *
     * @param key
     * @param valueIter
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @throws ReducerException
     */
    @Override
    protected void doReduce(Text key, Iterable<LongWritable> valueIter, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException, ReducerException {
        Iterator<LongWritable> iter = valueIter.iterator();
        long sum = 0;
        while (iter.hasNext()) {
            long v = iter.next().get();
            sum += v;
        }
        context.write(key, new LongWritable(sum));
    }
}
