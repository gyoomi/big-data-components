package com.gyoomi.hadoop.mapreduce.wordcountdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Word Count Driver
 *
 * @author Leon
 * @version 2020/8/29 15:11
 */
public class WordCountDriver
{

	public static void main(String[] args) throws Exception
	{
		if (null == args || args.length == 0)
		{
			throw new RuntimeException("argss 参数为空");
		}

		Configuration configuration = new Configuration();
		// configuration.set("mapreduce.framework.name", "yarn");
		// configuration.set("yarn.resoucemanager.hostname", "mini01");
		Job job = Job.getInstance(configuration);
		// job.setJar("/home/wc.jar");
		job.setJarByClass(WordCountDriver.class);


		// 指定本业务job要使用的mapper和reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// 指定mapper输出的数据类型kv
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 指定reducer输出的数据类型kv(也就是最终的输出的结果数据类型)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 指定job的输入的原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 指定job的输出结果文件所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		// job.submit()
		boolean flag = job.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}
}
