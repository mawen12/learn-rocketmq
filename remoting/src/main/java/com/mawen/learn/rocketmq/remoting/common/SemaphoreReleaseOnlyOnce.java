package com.mawen.learn.rocketmq.remoting.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class SemaphoreReleaseOnlyOnce {

	private final AtomicBoolean released = new AtomicBoolean(false);

	private final Semaphore semaphore;

	public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
		this.semaphore = semaphore;
	}

	public void release() {
		if (this.semaphore != null) {
			if (this.released.compareAndSet(false, true)) {
				this.semaphore.release();
			}
		}
	}

	public Semaphore getSemaphore() {
		return semaphore;
	}
}
