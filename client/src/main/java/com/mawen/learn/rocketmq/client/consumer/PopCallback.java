package com.mawen.learn.rocketmq.client.consumer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
public interface PopCallback {
	void onSuccess(final PopResult popResult);

	void onException(final Throwable e);
}
