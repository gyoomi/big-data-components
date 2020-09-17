/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.logenhancer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

/**
 * DB tool
 *
 * @author Leon
 * @date 2020-09-17 11:02
 */
public class DBLoader
{

	public static void dbLoader(Map<String, String> ruleMap) throws Exception {

		Connection conn = null;
		Statement st = null;
		ResultSet res = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/urldb", "root", "root");
			st = conn.createStatement();
			res = st.executeQuery("select url,content from url_rule");
			while (res.next()) {
				ruleMap.put(res.getString(1), res.getString(2));
			}

		} finally {
			try{
				if(res!=null){
					res.close();
				}
				if(st!=null){
					st.close();
				}
				if(conn!=null){
					conn.close();
				}

			}catch(Exception e){
				e.printStackTrace();
			}
		}

	}
}
