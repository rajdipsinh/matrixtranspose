package org.in.rajdip;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Date: 6/22/13
 * Time: 11:27 AM
 */
public class MatrixTransposeReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private final NullWritable value = NullWritable.get();
    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, value);
    }
}
