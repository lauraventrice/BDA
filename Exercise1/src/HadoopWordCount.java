import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopWordCount extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split(" ");

            String patternNumber = "^(\\d+)((\\.)(\\d+))?$"; // numbers' pattern
            String patterWord = "^([a-z]|[\\_]|[\\-]+)([\\-]|[\\_]|[a-z])*$"; // words' pattern

            Pattern rn = Pattern.compile(patternNumber); // compiling the pattern
            Pattern rw = Pattern.compile(patterWord);

            for (String w : splitLine) {
                Matcher m = rn.matcher(w);
                if(m.find()) { // check if we find a digit
                    word.set(m.group(0));
                    context.write(word, one);
                } else{
                    Matcher mw = rw.matcher(w.toLowerCase());
                    if(mw.find()){ // else it is a word
                        word.set(mw.group(0));
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values)
                sum += value.get();

            context.write(key, new IntWritable(sum));
        }
    }

    public static class ReverseComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
        public ReverseComparator() {
            super(Text.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return (-1)* TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof Text && b instanceof Text) {
                return (-1)*(((Text) a).compareTo((Text) b));
            }
            return super.compare(a, b);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        // Class for partitioning the output
        @Override
        public int getPartition(Text text, IntWritable intWritable, int numReduceTasks) {
            String str = text.toString();
            String patternNumber = "(\\d+)"; // numbers' pattern

            Pattern rn = Pattern.compile(patternNumber); // compiling the pattern

            Matcher m = rn.matcher(str);

            if(m.find()) { // file 0 - numbers, file 1 - words
                return 0;
            } else {
                return 1;
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "HadoopWordCount");
        job.setJarByClass(HadoopWordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setSortComparatorClass(ReverseComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(2); // use two different reducers for numbers or words

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        for(int i = 1; i<args.length-1;i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));


        return job.waitForCompletion(true) ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopWordCount(), args);
        System.exit(ret);
    }
}