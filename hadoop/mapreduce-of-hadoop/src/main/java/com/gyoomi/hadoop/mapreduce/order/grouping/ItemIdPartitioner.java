/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.order.grouping;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * id partitioner
 *
 * @author Leon
 * @date 2020-09-21 14:37
 */
public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable>
{
	@Override
	public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i)
	{
		Text itemId = orderBean.getItemId();
		int id = Integer.parseInt(StringUtils.substringAfter(itemId.toString(), "_"));
		return id % 3;
	}
}
