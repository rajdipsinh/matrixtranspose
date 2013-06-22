package org.in.rajdip;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Date: 6/22/13
 * Time: 12:11 PM
 */
public class InputParser {

    private static Text input = new Text();
    private static Text key = new Text();
    private static NullWritable value = NullWritable.get();

    private InputParser(){}

    public void setInput(Text input) {
        this.input = input;
    }

    public static Text getKey() {
        return key;
    }

    public static NullWritable getValue() {
        return value;
    }

    public static void parseInput(Text in) {
        input = in;
        String[] matValue = input.toString().split("\\s+");
        key.set(matValue[1] + " " + matValue[0] + " " + matValue[2]);
    }
}
