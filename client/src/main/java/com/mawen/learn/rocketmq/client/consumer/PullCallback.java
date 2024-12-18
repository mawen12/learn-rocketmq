package com.mawen.learn.rocketmq.client.consumer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
public interface PullCallback {

	void onSuccess(final PullResult pullResult);

	void onException(final Throwable e);
}
