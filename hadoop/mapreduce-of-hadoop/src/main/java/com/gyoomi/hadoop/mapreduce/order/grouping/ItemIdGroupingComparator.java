/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.order.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义grouping comparator: 用于控制shuffle过程中reduce端对kv对的聚合逻辑
 *
 * @author Leon
 * @date 2020-09-21 11:54
 */
public class ItemIdGroupingComparator extends WritableComparator
{

	protected ItemIdGroupingComparator()
	{
		super(OrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		OrderBean aOrderBean = (OrderBean) a;
		OrderBean bOrderBean = (OrderBean) b;
		// 将item_id相同的bean都视为相同，从而聚合为一组
		return aOrderBean.getItemId().compareTo(bOrderBean.getItemId());
	}
}
