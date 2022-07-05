package line;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class LineCount {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		Text line = new Text("Total Lines");
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { // Deer Bear Deer
		          context.write(line, new IntWritable(1)); 
		
	
		
		} }
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable totalsum = new IntWritable();
		
			
			
		
			public void reduce(Text key, Iterable<IntWritable> values,
				    Context context) throws IOException, InterruptedException {
				int sum = 0;
				   for (IntWritable val : values) {
				    sum += val.get();
				   }
				   
				   context.write(key, totalsum);
			}


		}
	
	
	public static void main(String[] args) throws Exception { 
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "line count"); 
	job.setJarByClass(LineCount.class); 
	job.setMapperClass(Map.class); 
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class); 
	job.setInputFormatClass(TextInputFormat.class); 
	job.setOutputFormatClass(TextOutputFormat.class); 
	FileInputFormat .setInputPaths(job, new Path(args[0])); 
	FileOutputFormat.setOutputPath(job, new Path(args[1])); 
    System.exit(job.waitForCompletion(true) ? 0 : 1); }
	
	
	
	
	}







