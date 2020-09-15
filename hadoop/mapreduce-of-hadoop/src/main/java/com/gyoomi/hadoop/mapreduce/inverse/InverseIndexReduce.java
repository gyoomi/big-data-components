/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.inverse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 倒排索引 - 2
 *
 * @author Leon
 * @date 2020-09-10 16:25
 */
public class InverseIndexReduce
{

	static class InverseIndexReduceMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			// hello>>1.txt 10
			String lineText = value.toString();
			String[] split = lineText.split(">>");
			// hello 1.txt 10
			String letter = split[0];
			String fileNameAndTimes = split[1];
			context.write(new Text(letter), new Text(fileNameAndTimes));
		}
	}

	static class InverseIndexReduceReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			StringBuffer sb = new StringBuffer();
			for (Text value : values)
			{
				sb.append(value.toString().replace("\t", "--->")).append("\t");
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length <= 0)
		{
			args = new String[]{"D:/bigdata/inverindexoutput", "D:/bigdata/inverindexoutput22"};
		}

		Configuration config = new Configuration();
		Job job = Job.getInstance(config);

		job.setMapperClass(InverseIndexReduceMapper.class);
		job.setReducerClass(InverseIndexReduceReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
