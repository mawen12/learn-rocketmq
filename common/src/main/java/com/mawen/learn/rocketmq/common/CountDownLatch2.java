package com.mawen.learn.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class CountDownLatch2 {

	private final Sync sync;

	public CountDownLatch2(int count) {
		if (count < 0) {
			throw new IllegalArgumentException("count < 0");
		}
		this.sync = new Sync(count);
	}

	public void await() throws InterruptedException {
		sync.acquireSharedInterruptibly(1);
	}

	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
	}

	public void countDown() {
		sync.releaseShared(1);
	}

	public long getCount() {
		return sync.getCount();
	}

	public void reset() {
		sync.reset();
	}

	public String toString() {
		return super.toString() + "[Count = " + sync.getCount() + "]";
	}

	private static final class Sync extends AbstractQueuedSynchronizer {
		private static final long serialVersionUID = 5209344007500811853L;

		private final int startCount;

		Sync(int count) {
			this.startCount = count;
			setState(count);
		}

		int getCount() {
			return getState();
		}

		@Override
		protected int tryAcquireShared(int arg) {
			return getState() == 0 ? 1 : -1;
		}

		@Override
		protected boolean tryReleaseShared(int arg) {
			for (; ; ) {
				int c = getState();
				if (c == 0) {
					return false;
				}

				int nextc = c - 1;
				if (compareAndSetState(c, nextc)) {
					return nextc == 0;
				}
			}
		}

		protected void reset() {
			setState(startCount);
		}
	}
}
