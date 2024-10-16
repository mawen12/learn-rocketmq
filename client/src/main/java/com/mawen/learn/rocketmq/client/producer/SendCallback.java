package com.mawen.learn.rocketmq.client.producer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public interface SendCallback {

	void onSuccess(final SendResult sendResult);

	void onException(final Throwable e);
}
