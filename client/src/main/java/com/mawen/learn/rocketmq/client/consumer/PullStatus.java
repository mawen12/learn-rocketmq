package com.mawen.learn.rocketmq.client.consumer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
public enum PullStatus {
	FOUND,

	NO_NEW_MSG,

	NO_MATCHED_MSG,

	OFFSET_ILLEGAL;
}
