package com.mawen.learn.rocketmq.store;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
public interface PutMessageLock {

	void lock();

	void unlock();
}
