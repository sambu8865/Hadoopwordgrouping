package hadoopprjt2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class prjt2 {

	public static class MapperOne extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text("");
		private Text file = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			FileSplit split = (FileSplit) reporter.getInputSplit();

			String name = split.getPath().getName();
			//String name="filename";

			file.set(name + "@" + key);

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().toString());
				output.collect(new Text(file), word);
			}

		}

	}// end of mapper

	public static class ReducerOne extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			ArrayList<String> li = new ArrayList<String>();
			
			while (values.hasNext()) {
				li.add(values.next().toString());
			}
			int size=li.size();
			Collections.sort(li);
			for(int i=0;i<size-1;i++) {
				for(int j=i+1;j<size;j++){				
					if (!li.get(i).equals(li.get(j))) {
						output.collect(new Text(li.get(i)+ " " + li.get(j)), null);
					}
				}
			}
			li.clear();
		}

	}// end of reducer
	
	public static class Mappertwo extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
@Override
public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter repoter)
		throws IOException {

	
		output.collect(new Text(value.toString()), new IntWritable(1));
	

}

}

public static class Reducertwo extends MapReduceBase implements 
	Reducer<Text, IntWritable, Text, IntWritable> {
@Override
public void reduce(Text key, Iterator<IntWritable> values,
		OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException {
	int sum = 0;
	while (values.hasNext()) {
		sum++;
		values.next();
	}
	String[] str = key.toString().split(" ");

		output.collect(new Text(str[0] + " appears " + sum
				+ " times with " + str[1]), null);

}
}

	public static void main(String[] args) throws Exception {
		// Path output = new Path("HadoopLab1");
		// File temp = new File(output.toString());
		Path output = new Path("Hadoopprjt1");
		File temp = new File(output.toString());

		if (temp.isDirectory() && temp.exists()) {
			FileUtils.deleteDirectory(temp);
			System.out.println("Deleting Folder");
		}

		Date start = new Date();
		System.out
				.println("Start........................... Program starting time: "
						+ start);

		//Configuration conf = new Configuration();
		System.out.println("Started Job");
		// JobConf job = new JobConf(prjt2.class);
		// JobClient job = new JobClient(conf, "MapReduce 1");

		JobConf job = new JobConf(prjt2.class);
		// job.setJobName("Driver");

		job.setJarByClass(prjt2.class);
		job.setMapperClass(prjt2.MapperOne.class);
		job.setReducerClass(prjt2.ReducerOne.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		/*
		 * job.waitForCompletion(true);
		 * 
		 * Job job2 = new Job(conf, "MapReduce 2");
		 * job2.setJarByClass(prjt2.class);
		 * 
		 * job2.setMapperClass(prjt1.Mappertwo.class);
		 * job2.setReducerClass(prjt1.Reducertwo.class);
		 * 
		 * job2.setOutputKeyClass(Text.class);
		 * job2.setOutputValueClass(IntWritable.class);
		 * 
		 * job2.setMapOutputKeyClass(Text.class);
		 * job2.setMapOutputValueClass(Text.class);
		 * job2.setOutputKeyClass(Text.class);
		 * job2.setOutputValueClass(NullWritable.class);
		 * 
		 * org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job
		 * , new Path(args[1]));
		 * org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
		 * .setOutputPath(job, new Path(args[2]));
		 * 
		 * job2.waitForCompletion(true);
		 */
		// job.waitForCompletion(true);

		Date finish = new Date();
		
		System.out.println("Program ending time  " + finish);
		Long diff = (finish.getTime() - start.getTime());
		System.out.println("Driver Programs start time is: " + start
				+ " , and end time is: " + finish);
		System.out.println("Total time of execution: " + diff
				+ " milliseconds.");
		
		
		JobConf job1 = new JobConf(prjt2.class);
		// job.setJobName("Driver");

		job1.setJarByClass(prjt2.class);
		job1.setMapperClass(prjt2.Mappertwo.class);
		job1.setReducerClass(prjt2.Reducertwo.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		JobClient.runJob(job1);
		
		Date finish11 = new Date();

		System.out.println("Program ending time  " + finish11);
		Long diff1 = (finish11.getTime() - start.getTime());
		System.out.println("Driver Programs start time is: " + start
				+ " , and end time is: " + finish11);
		System.out.println("Total time of execution: " + diff1
				+ " milliseconds.");

	}

}
