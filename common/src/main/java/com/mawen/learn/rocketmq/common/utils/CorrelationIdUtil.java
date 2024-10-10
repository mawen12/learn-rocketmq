package com.mawen.learn.rocketmq.common.utils;

import java.util.UUID;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class CorrelationIdUtil {

	public static String createCorrelationId() {
		return UUID.randomUUID().toString();
	}
}
