package com.mawen.learn.rocketmq.common;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public interface UnlockCallback {

	void onSuccess();

	void onException(final Throwable e);
}
