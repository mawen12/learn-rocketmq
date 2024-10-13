package com.mawen.learn.rocketmq.common.consumer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public enum ConsumeFromWhere {

	CONSUME_FROM_LAST_OFFSET,

	CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,

	CONSUME_FROM_MIN_OFFSET,

	CONSUME_FROM_MAX_OFFSET,

	CONSUME_FROM_FIRST_OFFSET,

	CONSUME_FROM_TIMESTAMP;
}
