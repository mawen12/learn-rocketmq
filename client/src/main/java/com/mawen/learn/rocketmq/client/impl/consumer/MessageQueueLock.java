package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class MessageQueueLock {

	private ConcurrentMap<MessageQueue, ConcurrentMap<Integer, Object>> mqLockTable = new ConcurrentHashMap<>();

	public Object fetchLockObject(final MessageQueue mq) {
		return fetchLockObject(mq, -1);
	}

	public Object fetchLockObject(final MessageQueue mq, final int shardingKeyIndex) {
		ConcurrentMap<Integer, Object> objMap = this.mqLockTable.computeIfAbsent(mq, key -> new ConcurrentHashMap<>(32));

		Object lock = objMap.computeIfAbsent(shardingKeyIndex, k -> new Object());

		return lock;
	}
}
