package com.mawen.learn.rocketmq.common.concurrent;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class ThreadPoolQueueSizeMonitor implements ThreadPoolStatusMonitor {

	private final int maxQueueCapacity;

	public ThreadPoolQueueSizeMonitor(int maxQueueCapacity) {
		this.maxQueueCapacity = maxQueueCapacity;
	}

	@Override
	public String describe() {
		return "queueSize";
	}

	@Override
	public double value(ThreadPoolExecutor executor) {
		return executor.getQueue().size();
	}

	@Override
	public boolean needPrintJstack(ThreadPoolExecutor executor, double value) {
		return value > maxQueueCapacity * 0.85;
	}
}
