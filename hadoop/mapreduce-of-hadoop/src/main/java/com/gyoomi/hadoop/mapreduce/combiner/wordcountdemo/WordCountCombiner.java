/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.combiner.wordcountdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The combiner of word count
 *
 * 输入为map的输出
 *
 * （1）combiner是MR程序中Mapper和Reducer之外的一种组件
 * （2）combiner组件的父类就是Reducer
 * （3）combiner和reducer的区别在于运行的位置：
 *      Combiner是在每一个maptask所在的节点运行
 *      Reducer是接收全局所有Mapper的输出结果；
 *  (4) combiner的意义就是对每一个maptask的输出进行局部汇总，以减小网络传输量
 *
 * @author Leon
 * @date 2020-09-03 18:05
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int counter = 0;
		for (IntWritable wordCountTime : values)
		{
			counter += wordCountTime.get();
		}

		context.write(key, new IntWritable(counter));
	}
}
