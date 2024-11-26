package com.mawen.learn.rocketmq.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class ThreadFactoryImpl implements ThreadFactory {

	private static final Logger log = LoggerFactory.getLogger(ThreadFactoryImpl.class);

	private final AtomicLong threadIndex = new AtomicLong(0);
	private final String threadNamePrefix;
	private final boolean daemon;

	public ThreadFactoryImpl(String threadNamePrefix) {
		this(threadNamePrefix, false);
	}

	public ThreadFactoryImpl(String threadNamePrefix, boolean daemon) {
		this.threadNamePrefix = threadNamePrefix;
		this.daemon = daemon;
	}

	public ThreadFactoryImpl(String threadNamePrefix, BrokerIdentity brokerIdentity) {
		this(threadNamePrefix, false, brokerIdentity);
	}

	public ThreadFactoryImpl(String threadNamePrefix, boolean daemon, BrokerIdentity brokerIdentity) {
		this.daemon = daemon;
		if (brokerIdentity != null && brokerIdentity.isInBrokerContainer()) {
			this.threadNamePrefix = brokerIdentity.getIdentifier() + threadNamePrefix;
		}
		else {
			this.threadNamePrefix = threadNamePrefix;
		}
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
		thread.setDaemon(daemon);

		thread.setUncaughtExceptionHandler((t, e) -> {
			log.error("[BUG] Thread has an uncaught exception, threadId={}, threadName={}", t.getId(), t.getName(), e);
		});

		return thread;
	}
}
