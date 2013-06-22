package org.in.rajdip;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MatrixTransposeMapper
 * Input: ROW COLUMN VALUE
 * Output:
 * Created with IntelliJ IDEA.
 * Date: 6/22/13
 * Time: 11:09 AM
 */
public class MatrixTransposeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final Text outKey = new Text();
    private final NullWritable outVal = NullWritable.get();

    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputParser.parseInput(value);
        context.write(InputParser.getKey(), InputParser.getValue());
    }
}
