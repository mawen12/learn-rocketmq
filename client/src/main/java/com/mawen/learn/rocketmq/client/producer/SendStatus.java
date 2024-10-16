package com.mawen.learn.rocketmq.client.producer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public enum SendStatus {

	SEND_OK,

	FLUSH_DISK_TIMEOUT,

	FLUSH_SLAVE_TIMEOUT,

	SLAVE_NOT_AVAILABLE;
}
