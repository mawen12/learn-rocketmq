package com.mawen.learn.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public abstract class LifeCycleAwareServiceThread extends ServiceThread{

	private final AtomicBoolean started = new AtomicBoolean(false);

	@Override
	public void run() {
		started.set(true);
		synchronized (started) {
			started.notifyAll();
		}

		run0();
	}

	public abstract void run0();

	public void awaitStarted(long timeout) throws InterruptedException {
		long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
		synchronized (started) {
			while (!started.get()) {
				long duration = expires - System.nanoTime();
				if (duration < TimeUnit.MICROSECONDS.toNanos(1)) {
					break;
				}
				started.wait(TimeUnit.NANOSECONDS.toMillis(duration));
			}
		}
	}
}
