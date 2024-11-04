package com.mawen.learn.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public abstract class ReferenceResource {

	protected final AtomicLong refCount = new AtomicLong(1);

	@Getter
	protected volatile boolean available = true;

	protected volatile boolean cleanupOver = false;

	protected volatile long firstShutdownTimestamp = 0;

	public abstract boolean cleanup(final long currentRef);

	public synchronized boolean hold() {
		if (isAvailable()) {
			if (refCount.getAndIncrement() > 0) {
				return true;
			}
			else {
				refCount.getAndDecrement();
			}
		}
		return false;
	}

	public void shutdown(final long intervalForcibly) {
		if (available) {
			available = false;
			firstShutdownTimestamp = System.currentTimeMillis();
			release();
		}
		else if (getRefCount() > 0) {
			if (System.currentTimeMillis() - firstShutdownTimestamp >= intervalForcibly) {
				refCount.set(-1000 - getRefCount());
				release();
			}
		}
	}

	public void release() {
		long value = refCount.decrementAndGet();
		if (value > 0) {
			return;
		}

		synchronized (this) {
			cleanupOver = this.cleanup(value);
		}
	}

	public long getRefCount() {
		return refCount.get();
	}

	public boolean isCleanupOver() {
		return refCount.get() <= 0 && cleanupOver;
	}
}
