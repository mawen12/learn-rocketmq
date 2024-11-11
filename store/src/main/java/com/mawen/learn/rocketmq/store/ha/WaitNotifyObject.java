package com.mawen.learn.rocketmq.store.ha;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mawen.learn.rocketmq.common.utils.ConcurrentHashMapUtils;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
public class WaitNotifyObject {

	private static final Logger log = LoggerFactory.getLogger(WaitNotifyObject.class);

	private final ConcurrentMap<Long, AtomicBoolean> waitingThreadTable = new ConcurrentHashMap<>(16);

	protected AtomicBoolean hasNotified = new AtomicBoolean(false);

	public void wakeup() {
		boolean needNotify = hasNotified.compareAndSet(false, true);
		if (needNotify) {
			synchronized (this) {
				this.notify();
			}
		}
	}

	protected void waitForRunning(long interval) {
		if (hasNotified.compareAndSet(true, false)) {
			onWaitEnd();
			return;
		}
		synchronized (this) {
			try {
				if (hasNotified.compareAndSet(true, false)) {
					onWaitEnd();
					return;
				}
				wait(interval);
			}
			catch (InterruptedException e) {
				log.error("Interrupted", e);
			}
			finally {
				hasNotified.set(false);
				onWaitEnd();
			}
		}
	}

	protected void onWaitEnd() {
		// NOP
	}

	public void wakeupAll() {
		boolean needNotify = false;
		for (Map.Entry<Long, AtomicBoolean> entry : waitingThreadTable.entrySet()) {
			if (entry.getValue().compareAndSet(false, true)) {
				needNotify = true;
			}
		}
		if (needNotify) {
			synchronized (this) {
				notifyAll();
			}
		}
	}

	public void allWaitForRunning(long interval) {
		long currentThreadId = Thread.currentThread().getId();
		AtomicBoolean notified = ConcurrentHashMapUtils.computeIfAbsent(waitingThreadTable, currentThreadId, k -> new AtomicBoolean(false));
		if (notified.compareAndSet(true, false)) {
			onWaitEnd();
			return;
		}

		synchronized (this) {
			try {
				if (notified.compareAndSet(true, false)) {
					onWaitEnd();
					return;
				}
				wait(interval);
			}
			catch (InterruptedException e) {
				log.error("interrupted", e);
			}
			finally {
				notified.set(false);
				onWaitEnd();
			}
		}
	}

	public void removeFromWaitingThreadTable() {
		long currentThreadId = Thread.currentThread().getId();
		synchronized (this) {
			waitingThreadTable.remove(currentThreadId);
		}
	}
}
