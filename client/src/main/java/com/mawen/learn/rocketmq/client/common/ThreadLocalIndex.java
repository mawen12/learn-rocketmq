package com.mawen.learn.rocketmq.client.common;

import java.util.Random;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
public class ThreadLocalIndex {

	private final static int POSITIVE_MASK = 0x7FFFFFFF;
	private final Random random = new Random();
	private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<>();

	public int incrementAndGet() {
		Integer index = threadLocalIndex.get();
		if (index == null) {
			index = random.nextInt();
		}

		threadLocalIndex.set(++index);
		return index & POSITIVE_MASK;
	}

	public void reset() {
		int index = Math.abs(random.nextInt(Integer.MAX_VALUE));
		threadLocalIndex.set(index);
	}

	@Override
	public String toString() {
		return "ThreadLocalIndex{" +
				"threadLocalIndex=" + threadLocalIndex.get() +
				'}';
	}
}
