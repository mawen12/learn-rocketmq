package com.mawen.learn.rocketmq.client.consumer.store;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public enum ReadOffsetType {

	READ_FROM_MEMORY,

	READ_FROM_STORE,

	MEMORY_FIRST_THEN_STORE;
}
