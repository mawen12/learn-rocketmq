package com.mawen.learn.rocketmq.common;

import java.util.Set;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public interface LockCallback {

	void onSuccess(final Set<MessageQueue> lockOKMQSet);

	void onException(final Throwable e);
}
