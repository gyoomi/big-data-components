package com.gyoomi.hadoop.mapreduce.wordcountdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Word Count Demo <p>
 *
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN: 是mr框架所读到的一行文本的起始偏移量，默认是Long
 *     VALUEIN：是mr框架所读到的一行文本的内容，String
 *     KEYOUT： 是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String
 *     VALUEOUT： 是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer
 *
 * @author Leon
 * @version 2020/8/29 14:39
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

	/**
	 * map阶段的业务逻辑就写在自定义的map()方法中.
	 *
	 * maptask会对每一行输入数据调用一次我们自定义的map()方法
	 *
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// 1. 将mapTask传给我们的文本内容先转换成String
		String lineText = value.toString();
		// 2. 按照空格将这一行切分成单词
		String[] words = lineText.split(" ");
		// 3. 将单词输出为<单词，1>
		for (String word : words)
		{
			//将单词作为key，将次数1作为value，以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reduce task
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
