/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.inverse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 倒排索引
 *
 * @author Leon
 * @date 2020-09-10 9:36
 */
public class InverseIndexCount
{

	static class InverseIndexCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String lineText = value.toString();
			String[] words = lineText.split(" ");
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			for (String word : words)
			{
				// hello>>1.txt 1
				Text wordKey = new Text(word + ">>" + fileName);
				context.write(wordKey, new IntWritable(1));
			}
		}
	}

	static class InverseIndexCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int counter = 0;
			for (IntWritable value : values)
			{
				counter += value.get();
			}
			// hello>>1.txt 10
			// hello>>2.txt 8
			// tom>>1.txt 10
			// tom>>2.txt 1
			context.write(key, new IntWritable(counter));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(InverseIndexCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(InverseIndexCountMapper.class);
		job.setReducerClass(InverseIndexCountReducer.class);

		job.waitForCompletion(true);
	}


}
