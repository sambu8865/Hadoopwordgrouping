package hadoopprjt2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class prjt1 extends Configured implements Tool {

	public static class MapperOne extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text file;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException {

			try {
				setup(context);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			// FileSplit split = (FileSplit) reporter.getInputSplit();

			// String name = split.getPath().getName();

			file = new Text(fileName + "@" + key.toString() + ":" + word);

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().toString());
				try {
					context.write(file, word);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}// end of mapper

	public static class KeyComprator extends WritableComparator {

		protected KeyComprator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			// ascending zone and day

			Text t1 = (Text) w1;
			Text t2 = (Text) w2;
			String[] t1Items = t1.toString().split(":");
			String[] t2Items = t2.toString().split(":");
			String t1Base = t1Items[0] + ":" + t1Items[1] + ":";
			String t2Base = t2Items[0] + ":" + t2Items[1] + ":";
			int comp = t1Base.compareTo(t2Base);

			// descending value
			if (comp == 0) {
				comp = t2Items[2].compareTo(t1Items[2]);
			}

			return comp;

		}
	}

	public static class GroupComprator extends WritableComparator {

		protected GroupComprator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			// consider only zone and day part of the key
			Text t1 = (Text) w1;
			Text t2 = (Text) w2;
			String[] t1Items = t1.toString().split(":");
			String[] t2Items = t2.toString().split(":");
			String t1Base = t1Items[0] + ":" + t1Items[1] + ":";
			String t2Base = t2Items[0] + ":" + t2Items[1] + ":";
			int comp = t1Base.compareTo(t2Base);

			return comp;

		}
	}

	public static class ReducerOne extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			ArrayList<String> li = new ArrayList<String>();
			StringBuilder stringbldr = new StringBuilder();
			stringbldr = stringbldr.append(values.next().toString());
			while (values.hasNext()) {
				li.add(values.next().toString());
			}
			Collections.sort(li);
			ListIterator<String> first = li.listIterator();
			ListIterator<String> second = li.listIterator();
			while (first.hasNext()) {
				String current = first.next();
				second = first;
				while (second.hasNext()) {
					String str = second.next();
					if (!current.equals(str)) {
						context.write(new Text(current + " " + str),
								new IntWritable());
					}
				}
			}

		}

	}// end of reducer

	public static class Mappertwo extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException {

			try {
				context.write(new Text(value.toString()), new IntWritable(1));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public static class Reducertwo extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				Context context) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum++;
				values.next();
			}
			String[] str = key.toString().split(" ");
			try {
				context.write(new Text(str[0] + " appears " + sum
						+ " times with " + str[1]), new IntWritable());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/*
	 * job.setMapperClass(MapperOne.class);
	 * job.setReducerClass(ReducerOne.class);
	 * 
	 * job.setOutputKeyClass(Text.class);
	 * job.setOutputValueClass(IntWritable.class);
	 * 
	 * job.setInputFormat(TextInputFormat.class);
	 * job.setOutputFormat(TextOutputFormat.class);
	 * 
	 * job.setMapOutputKeyClass(Text.class);
	 * job.setMapOutputValueClass(Text.class);
	 * job.setOutputKeyClass(Text.class);
	 * job.setOutputValueClass(NullWritable.class);
	 * 
	 * FileInputFormat.addInputPath(job, new Path(args[0]));
	 * FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 * JobClient.runJob(job);
	 * 
	 * Date finish = new Date();
	 * 
	 * System.out.println("Program ending time  " + finish); Long diff =
	 * (finish.getTime() - start.getTime());
	 * System.out.println("Driver Programs start time is: " + start +
	 * " , and end time is: " + finish);
	 * System.out.println("Total time of execution: " + diff +
	 * " milliseconds.");
	 * 
	 * }
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, "MapReduce 1");
		job.setJarByClass(prjt1.class);

		job.setMapperClass(prjt1.MapperOne.class);
		job.setReducerClass(prjt1.ReducerOne.class);
		job.setOutputKeyClass(Text.class); // Text
		job.setOutputValueClass(IntWritable.class); // IntWritable
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
				new Path(args[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
				job, new Path(args[1]));

		job.waitForCompletion(true);
		System.out.println("JOB 1 COMPLETE");
		// return job.waitForCompletion(true) ? 0 : 1;
		Job job2 = new Job(conf, "MapReduce 2");
		job2.setJarByClass(prjt1.class);
		job2.setMapperClass(prjt1.Mappertwo.class);
		job2.setReducerClass(prjt1.Reducertwo.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(
				job2, new Path(args[1]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
				job2, new Path(args[2]));

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(), new prjt1(), args);
		System.exit(res);

	}

}