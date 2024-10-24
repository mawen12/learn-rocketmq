package com.mawen.learn.rocketmq.client.consumer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface AckCallback {

	void onSuccess(final AckResult ackResult);

	void onException(final Throwable e);
}