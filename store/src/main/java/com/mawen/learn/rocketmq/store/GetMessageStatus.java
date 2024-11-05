package com.mawen.learn.rocketmq.store;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public enum GetMessageStatus {

	FOUND,

	NO_MATCHED_MESSAGE,

	MESSAGE_WAS_REMOVING,

	OFFSET_FOUND_NULL,

	OFFSET_OVERFLOW_BADLY,

	OFFSET_OVERFLOW_ONE,

	OFFSET_TOO_SMALL,

	NO_MATCHED_LOGIC_QUEUE,

	NO_MESSAGE_IN_QUEUE,

	OFFSET_RESET;
}
