package com.mawen.learn.rocketmq.store;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
public class PutMessageReentrantLock implements PutMessageLock {
	// Non fair sync
	private ReentrantLock putMessageNormalLock = new ReentrantLock();

	@Override
	public void lock() {
		putMessageNormalLock.lock();
	}

	@Override
	public void unlock() {
		putMessageNormalLock.unlock();
	}
}
