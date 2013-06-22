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
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * Date: 6/22/13
 * Time: 11:31 AM
 */
public class MatrixTransposeDriver extends Configured implements Tool {
    private enum ARGUMENTS {
        PARSERLIB("Parser Library"),
        INPUTFILE("Input File"),
        OUTPUTFILE("Output File");

        private final String text;
        ARGUMENTS(String argument) {
            this.text = argument;
        }

        @Override
        public String toString() {
            return text;
        }
    };
    private static Logger logger = Logger.getLogger(MatrixTransposeDriver.class);


    public static void printError(int i) {
        logger.error("Need to pass argument for " + ARGUMENTS.values()[i]);
    }

    public static void printUsage() {
        logger.error("General option supported\n"
                + "-parserlibjar -libjar, -lib, -l     : Library name including path"
                + "-inputpath, -in, -i                 : Input file name including path"
                + "-outputpath, -out, -o               : Output folder name including path"
        );

    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 6) {
            printUsage();
            return -1;
        }

        String [] argArray = new String[3];

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-parserlib") || args[i].equals("-lib") || args[i].equals("-libjars")
                    || args[i].equals("-l")) {
                argArray[ARGUMENTS.PARSERLIB.ordinal()] = args[i + 1];
            } else if (args[i].equals("-inputpath") || args[i].equals("-in") || args[i].equals("-i")) {
                argArray[ARGUMENTS.INPUTFILE.ordinal()] = args[i + 1];
            } else if (args[i].equals("-outputpath") || args[i].equals("-out") || args[i].equals("-o")) {
                argArray[ARGUMENTS.OUTPUTFILE.ordinal()] = args[i + 1];
            }
        }

        for (int i = 0; i < argArray.length; i++) {
            if (argArray[i].isEmpty()) {
                printError(i);
                return -1;
            }
        }

        Path sampleJarPath = new Path(argArray[ARGUMENTS.PARSERLIB.ordinal()]);
        FileSystem fileSystem = FileSystem.get(getConf());
        DistributedCache.addFileToClassPath(sampleJarPath, getConf(), fileSystem);


        Job job = Job.getInstance(getConf(), "Matrix Transpose");
        job.setJarByClass(MatrixTransposeDriver.class);

        Path inputPath = new Path(argArray[ARGUMENTS.INPUTFILE.ordinal()]);
        Path outputPath = new Path(argArray[ARGUMENTS.OUTPUTFILE.ordinal()]);
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
