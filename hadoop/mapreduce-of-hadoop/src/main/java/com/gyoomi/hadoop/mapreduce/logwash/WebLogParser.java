/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.logwash;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * WebLog Parser
 *
 * @author Leon
 * @date 2020-09-16 11:49
 */
public class WebLogParser
{

	static SimpleDateFormat sd1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);

	static SimpleDateFormat sd2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static WebLogBean parse(String line)
	{
		WebLogBean webLogBean = new WebLogBean();
		try {
			String[] arr = line.split(" ");
			if (arr.length > 11) {
				webLogBean.setRemoteAddr(arr[0]);
				webLogBean.setRemoteUser(arr[1]);
				webLogBean.setTimeLocal(parseTime(arr[3].substring(1)));
				webLogBean.setRequest(arr[6]);
				webLogBean.setStatus(arr[8]);
				webLogBean.setBodyBytesSent(arr[9]);
				webLogBean.setHttpReferer(arr[10]);

				if (arr.length > 12) {
					webLogBean.setHttpUserAgent(arr[11] + " " + arr[12]);
				} else {
					webLogBean.setHttpUserAgent(arr[11]);
				}
				if (Integer.parseInt(webLogBean.getStatus()) >= 400) {// 大于400，HTTP错误
					webLogBean.setValid(false);
				}
			} else {
				webLogBean.setValid(false);
			}
		} catch (Exception e) {
			webLogBean.setValid(true);
			e.printStackTrace();
		}
		return webLogBean;
	}

	public static String parseTime(String dt) {

		String timeString = "";
		try {
			Date parse = sd1.parse(dt);
			timeString = sd2.format(parse);

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return timeString;
	}

}
