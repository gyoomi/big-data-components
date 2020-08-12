/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.rpc.publish;

import com.gyoomi.hadoop.rpc.LoginServiceInterface;
import com.gyoomi.hadoop.rpc.impl.LoginServiceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * 服务发布
 *
 * @author Leon
 * @date 2020-08-12 9:02
 */
public class Publish
{

	public static void main(String[] args) throws Exception
	{
		RPC.Builder builder = new RPC.Builder(new Configuration());
		builder
			.setBindAddress("localhost")
			.setPort(13144)
			.setProtocol(LoginServiceInterface.class)
			.setInstance(new LoginServiceImpl());

		RPC.Server server = builder.build();
		server.start();
		System.out.println("server启动了...");
	}

}
