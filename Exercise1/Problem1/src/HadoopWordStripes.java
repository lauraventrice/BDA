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

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopWordStripes extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		private final static IntWritable one = new IntWritable(1);

		int maxDistance = 5; // max distance between tokens

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] splitLine = value.toString().split(" ");

			String patternNumber = "^(\\d+)((\\.)(\\d+))?$"; // numbers' pattern
			String patterWord = "^([a-z]|[\\_]|[\\-]+)([\\-]|[\\_]|[a-z])*$"; // words' pattern

			Pattern rn = Pattern.compile(patternNumber); // compiling the pattern
			Pattern rw = Pattern.compile(patterWord);

			for (int i = 0; i < splitLine.length; i++) {
				MapWritable map = new MapWritable();
				String w;

				if (i > 0) {
					for(int k = 1; k <= maxDistance && i-k>=0; k++) {
						w = splitLine[i - k];
						stripe(w, map);
					}
				}

				if (i < splitLine.length - 1) {
					for(int k = 1; k <= maxDistance && k+i<splitLine.length; k++){
						w = splitLine[i + k];
						stripe(w, map);
					}
				}

				Matcher m = rn.matcher(splitLine[i]);
				if (m.find()) { // check if we find a digit
					context.write(new Text(m.group(0)), map);
				} else {
					Matcher mw = rw.matcher(splitLine[i]);
					if (mw.find()) { // else it is a word
						context.write(new Text(mw.group(0).toLowerCase()), map);
					}
				}
			}
		}

		public static void stripe(String w, MapWritable map) {
			LongWritable count = new LongWritable(0);

			if (map.containsKey(new Text(w))) {
				count = (LongWritable) map.get(new Text(w));
				map.remove(new Text(w));
			}

			count = new LongWritable(count.get() + one.get());
			map.put(new Text(w), count);
		}

	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable stripe = new MapWritable();

			for (MapWritable localStripe : values) {
				Iterator entries = localStripe.entrySet().iterator();

				while (entries.hasNext()) {
					java.util.Map.Entry thisEntry = (java.util.Map.Entry) entries.next();
					Text keyNeighbour = (Text) thisEntry.getKey();
					LongWritable value = (LongWritable) thisEntry.getValue();
					globalStripe(keyNeighbour, value, stripe);
				}
			}

			context.write(key, stripe);
		}

		public static void globalStripe(Text key, LongWritable value, MapWritable map) {
			LongWritable sum = new LongWritable(0);

			if (map.containsKey(key)) {
				sum = (LongWritable) map.get(key);
				map.remove(key);
			}

			sum = new LongWritable(sum.get() + value.get());
			map.put(key, sum);
		}
	}

	public static class PartitionerClass extends Partitioner<Text, MapWritable> {
		// Class for partitioning the output

		@Override
		public int getPartition(Text text, MapWritable mapWritable, int i) {
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
		Job job = Job.getInstance(new Configuration(), "HadoopWordStripes");
		job.setJarByClass(HadoopWordStripes.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

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
		FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordStripes(), args);
		System.exit(ret);
	}
}