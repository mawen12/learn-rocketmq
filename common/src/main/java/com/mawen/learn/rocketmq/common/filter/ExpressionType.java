package com.mawen.learn.rocketmq.common.filter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public class ExpressionType {

	public static final String SQL92 = "SQL92";

	public static final String TAG = "TAG";

	public static boolean isTagType(String type) {
		return type == null || "".equals(type) || TAG.equals(type);
	}
}
