package com.mawen.learn.rocketmq.store;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
public class PutMessageSpinLock implements PutMessageLock {

	private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

	@Override
	public void lock() {
		boolean flag;
		do {
			flag = putMessageSpinLock.compareAndSet(true, false);
		}
		while (!flag);
	}

	@Override
	public void unlock() {
		putMessageSpinLock.compareAndSet(false, true);
	}
}
