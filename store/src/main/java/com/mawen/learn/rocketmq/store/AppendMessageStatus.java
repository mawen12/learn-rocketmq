package com.mawen.learn.rocketmq.store;/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public enum AppendMessageStatus {

	PUT_OK,

	END_OF_FILE,

	MESSAGE_SIZE_EXCEEDED,

	PROPERTIES_SIZE_EXCEEDED,

	UNKNOWN_ERROR;
}
