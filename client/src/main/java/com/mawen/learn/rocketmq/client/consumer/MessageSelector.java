package com.mawen.learn.rocketmq.client.consumer;

import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MessageSelector {

	private String type;

	private String expression;

	public static MessageSelector bySql(String sql) {
		return new MessageSelector(ExpressionType.SQL92, sql);
	}

	public static MessageSelector byTag(String tag) {
		return new MessageSelector(ExpressionType.TAG, tag);
	}
}
