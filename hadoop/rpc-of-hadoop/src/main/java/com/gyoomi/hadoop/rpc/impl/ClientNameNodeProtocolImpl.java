/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.rpc.impl;

import com.gyoomi.hadoop.rpc.ClientNameNodeProtocol;

/**
 * The description of class
 *
 * @author Leon
 * @date 2020-08-12 15:16
 */
public class ClientNameNodeProtocolImpl implements ClientNameNodeProtocol
{

	@Override
	public String getMetaData(String path)
	{
		return path + "@[blk_01,blk_02,blk_03]\n{blk_01:node01,node02}";
	}
}
