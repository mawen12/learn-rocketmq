package com.mawen.learn.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public abstract class ServiceThread implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(ServiceThread.class);

	private static final long JOIN_TIME = 90 * 1000;

	private final AtomicBoolean started = new AtomicBoolean(false);

	protected Thread thread;
	protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
	protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
	protected volatile boolean stopped = false;
	protected boolean isDaemon = false;

	public ServiceThread() {
	}

	public abstract String getServiceName();

	public void start() {
		log.info("Try to start service thread: {} started: {} lastThread: {}", getServiceName(), started.get(), thread);

		if (!started.compareAndSet(false, true)) {
			return;
		}

		stopped = false;
		thread = new Thread(this, getServiceName());
		thread.setDaemon(isDaemon);
		thread.start();

		log.info("Start service thread: {} started: {} lastThread: {}", getServiceName(), started.get(), thread);
	}

	public void shutdown() {
		this.shutdown(false);
	}

	public void shutdown(final boolean interrupt) {
		log.info("Try to shutdown service thread: {} started: {} lastThread: {}", getServiceName(), started.get(), thread);

		if (!started.compareAndSet(true, false)) {
			return;
		}
		stopped = true;

		log.info("shutdown thread[{}] interrupt={}", getServiceName(), interrupt);

		wakeup();

		try {
			if (interrupt) {
				this.thread.interrupt();
			}

			long beginTime = System.currentTimeMillis();
			if (!this.thread.isDaemon()) {
				this.thread.join(this.getJoinTime());
			}
			long elapsedTime = System.currentTimeMillis() - beginTime;

			log.info("join thread[{}], elapsed time: {}ms, join time: {}", getServiceName(), elapsedTime, getJoinTime());
		}
		catch (InterruptedException e) {
			log.error("Interrupted", e);
		}
	}

	public long getJoinTime() {
		return JOIN_TIME;
	}

	public void makeStop() {
		if (!started.get()) {
			return;
		}
		this.stopped = true;
		log.info("makeStop thread[{}]", this.getServiceName());
	}

	public void wakeup() {
		if (hasNotified.compareAndSet(false, true)) {
			waitPoint.countDown();
		}
	}

	protected void waitForRunning(long interval) {
		if (hasNotified.compareAndSet(true, false)) {
			this.onWaitEnd();
			return;
		}

		waitPoint.reset();

		try {
			waitPoint.await(interval, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			log.error("Interrupted", e);
		}
		finally {
			hasNotified.set(false);
			this.onWaitEnd();
		}
	}

	protected void onWaitEnd() {}

	public boolean isStopped() {
		return stopped;
	}

	public boolean isDaemon() {
		return isDaemon;
	}

	public void setDaemon(boolean daemon) {
		this.isDaemon = daemon;
	}
}
