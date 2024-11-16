package com.mawen.learn.rocketmq.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public class TopicQueueLock {
	private static final int INITIALIZE_SIZE = 32;

	private final int size;
	private final List<Lock> lockList;

	public TopicQueueLock() {
		this(INITIALIZE_SIZE);
	}

	public TopicQueueLock(int size) {
		this.size = size;
		this.lockList = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			lockList.add(new ReentrantLock());
		}
	}

	public void lock(String topicQueueKey) {
		Lock lock = lockList.get((topicQueueKey.hashCode() & 0x7fffffff) % size);
		lock.lock();
	}

	public void unlock(String topicQueueKey) {
		Lock lock = lockList.get((topicQueueKey.hashCode() & 0x7fffffff) % size);
		lock.unlock();
	}
}
