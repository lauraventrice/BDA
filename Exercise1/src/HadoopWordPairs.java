import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class HadoopWordPairs extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();

		private Text current = new Text();
		int maxDistance = 2; // max distance between tokens

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] splitLine = value.toString().split(" ");

			String patternNumber = "(\\d+)((\\.)(\\d+))?"; // numbers' pattern
			String patterWord = "(\\w+)(([\\-])+(\\w+))*"; // words' pattern

			Pattern rn = Pattern.compile(patternNumber); // compiling the pattern
			Pattern rw = Pattern.compile(patterWord);
			Matcher mn, mw;
			int length = 0;

			for (int i = 0; i < splitLine.length; i++) {
				mn = rn.matcher(splitLine[i]);
				current.set(splitLine[i]);
				if(mn.find()) {

					for(int j = i + 1; j - i < maxDistance && j < splitLine.length; j++) {
						mn = rn.matcher(splitLine[j]);
						if(mn.find()) {
							pair.set(current + ":" + mn.group(0));
							context.write(pair, one);
						}
					}
				} else {
					mw = rw.matcher(splitLine[i]);
					if(mw.find()) {
						for(int j = i + 1; j - i < maxDistance && j < splitLine.length; j++) {
							mw = rw.matcher(splitLine[j]);
							if(mw.find()) {
								pair.set(current + ":" + mw.group(0));
								context.write(pair, one);
							}
						}
					}
				}

			}
/*

			int lengthWord;
			int lengthNumber;

			for (String w : splitLine) {
				Matcher m = rn.matcher(w);
				if(m.find()) { // check if we find a digit
					lengthNumber = lastNumber.getLength();
					if (lengthNumber > 0 && (lengthNumber + m.group(0).length()) < maxDistance) {
						pair.set(lastNumber + ":" + m.group(0));
						context.write(pair, one);
					}
					lastNumber.set(m.group(0));
				} else{
					Matcher mw = rw.matcher(w.toLowerCase());
					if(mw.find()){ // else it is a word
						lengthWord = lastWord.getLength();
						if(lengthWord > 0 && (lengthWord + mw.group(0).length()) < maxDistance) {
							pair.set(lastWord + ":" + mw.group(0));
							context.write(pair, one);
						}
						lastWord.set(mw.group(0));
					}
				}
			}*/
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

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs(), args);
		System.exit(ret);
	}
}