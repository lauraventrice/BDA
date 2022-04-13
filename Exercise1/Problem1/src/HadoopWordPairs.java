import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopWordPairs extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();

		private Text current = new Text();
		int maxDistance = 5; // max distance between tokens

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] splitLine = value.toString().split(" ");

			String patternNumber = "^(\\d+)((\\.)(\\d+))?$"; // numbers' pattern
			String patterWord = "^([a-z]|[\\_]|[\\-]+)([\\-]|[\\_]|[a-z])*$"; // words' pattern

			Pattern rn = Pattern.compile(patternNumber); // compiling the pattern
			Pattern rw = Pattern.compile(patterWord);
			Matcher mn, mw;

			for (int i = 0; i < splitLine.length; i++) {
				mn = rn.matcher(splitLine[i]);
				if(mn.find()) {
					current.set(mn.group(0));
					// check number until maxDistance
					for(int j = i + 1; j - i <= maxDistance && j < splitLine.length; j++) {
						mn = rn.matcher(splitLine[j]);
						if(mn.find()) {
							pair.set(current + ":" + mn.group(0));
							context.write(pair, one);
						}
					}
				} else {
					mw = rw.matcher(splitLine[i]);
					if(mw.find()) {
						current.set(mw.group(0));
						// check number until maxDistance
						for(int j = i + 1; j - i <= maxDistance && j < splitLine.length; j++) {
							mw = rw.matcher(splitLine[j]);
							if(mw.find()) {
								pair.set(current + ":" + mw.group(0));
								context.write(pair, one);
							}
						}
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
			}
			else
				return 1;
		}
	}

	public static class IntComparator extends WritableComparator {

		public IntComparator() {
			super(IntWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1,
						   byte[] b2, int s2, int l2) {

			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

			return v1.compareTo(v2) * (-1);
		}
	}

	public static class PartitionerClass2 extends Partitioner<IntWritable, Text> {
		// Class for partitioning the output

		@Override
		public int getPartition(IntWritable intWritable, Text text, int i) {
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

	public static class MapTask extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\t"); // This is the delimiter between Key and Value
			int valuePart = Integer.parseInt(tokens[1]);
			context.write(new IntWritable(valuePart), new Text(tokens[0]));
		}
	}

	public static class ReduceTask extends Reducer<IntWritable, Text, Text, IntWritable> {

		public void reduce(IntWritable key, Iterable<Text> list, Context context) throws IOException, InterruptedException {

			for (Text value : list) {
				context.write(value,key);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "HadoopWordPairs");
		job.setJarByClass(HadoopWordPairs.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(PartitionerClass.class);

		job.setNumReduceTasks(2); // use two different reducers for numbers or words

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		for(int i = 1; i<args.length-1;i++) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path("./tmp"));

		job.waitForCompletion(true);

		Configuration conf = new Configuration(true);

		// Create job
		Job job2 = Job.getInstance(conf, "Sorting");
		job2.setJarByClass(HadoopWordCount.class);

		// Setup MapReduce
		job2.setMapperClass(MapTask.class);
		job2.setReducerClass(ReduceTask.class);
		job2.setNumReduceTasks(2);

		// Specify key / value
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setSortComparatorClass(IntComparator.class);
		job2.setPartitionerClass(PartitionerClass2.class);

		// Input
		FileInputFormat.addInputPath(job2, new Path("./tmp"));
		job2.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job2, new Path(args[args.length-1]));
		job2.setOutputFormatClass(TextOutputFormat.class);

		// Execute job
		int code = job2.waitForCompletion(true) ? 0 : 1;

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path("./tmp"))) hdfs.delete(new Path("./tmp"), true);
		return code;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs(), args);
		System.exit(ret);
	}
}