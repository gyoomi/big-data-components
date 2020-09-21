/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.order.grouping;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 订单金额 - Bean
 *
 * @author Leon
 * @date 2020-09-21 11:10
 */
public class OrderBean implements WritableComparable<OrderBean>
{

	private Text itemId;

	private DoubleWritable amount;

	public OrderBean() {
	}

	public OrderBean(Text itemId, DoubleWritable amount) {
		this.itemId = itemId;
		this.amount = amount;
	}

	public void set(Text itemId, DoubleWritable amount) {
		this.itemId = itemId;
		this.amount = amount;
	}

	@Override
	public int compareTo(OrderBean o)
	{
		int itemOrderNum = this.itemId.compareTo(o.getItemId());
		if (itemOrderNum == 0)
		{
			itemOrderNum = - this.amount.compareTo(o.getAmount());
		}
		return itemOrderNum;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException
	{
		dataOutput.writeUTF(itemId.toString());
		dataOutput.writeDouble(amount.get());
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException
	{
		this.itemId = new Text(dataInput.readUTF());
		this.amount = new DoubleWritable(dataInput.readDouble());
	}

	@Override
	public String toString()
	{
		return itemId.toString() + "\t" + amount.get();
	}

	public Text getItemId() {
		return itemId;
	}

	public void setItemId(Text itemId) {
		this.itemId = itemId;
	}

	public DoubleWritable getAmount() {
		return amount;
	}

	public void setAmount(DoubleWritable amount) {
		this.amount = amount;
	}
}
