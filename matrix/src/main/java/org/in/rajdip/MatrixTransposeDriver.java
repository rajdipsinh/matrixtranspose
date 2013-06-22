package org.in.rajdip;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * Date: 6/22/13
 * Time: 11:31 AM
 */
public class MatrixTransposeDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Path sampleJarPath = new Path("/lib/org/in/rajdip/samplejar/samplejar.jar");
        FileSystem fileSystem = FileSystem.get(getConf());
        DistributedCache.addFileToClassPath(sampleJarPath, getConf(), fileSystem);


        Job job = Job.getInstance(getConf(), "Matrix Transpose");
        job.setJarByClass(MatrixTransposeDriver.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        fileSystem = FileSystem.get(outputPath.toUri(), getConf());
        fileSystem.delete(outputPath, true);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MatrixTransposeMapper.class);
        job.setReducerClass(MatrixTransposeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MatrixTransposeDriver(), args));
    }
}
