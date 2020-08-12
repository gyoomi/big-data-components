/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.rpc.publish;

import com.gyoomi.hadoop.rpc.ClientNameNodeProtocol;
import com.gyoomi.hadoop.rpc.LoginServiceInterface;
import com.gyoomi.hadoop.rpc.impl.ClientNameNodeProtocolImpl;
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

		RPC.Builder builder2 = new RPC.Builder(new Configuration());
		builder2
			.setBindAddress("localhost")
			.setPort(13145)
			.setProtocol(ClientNameNodeProtocol.class)
			.setInstance(new ClientNameNodeProtocolImpl());

		RPC.Server server2 = builder2.build();
		server2.start();
		System.out.println("server2启动了...");
	}

}
