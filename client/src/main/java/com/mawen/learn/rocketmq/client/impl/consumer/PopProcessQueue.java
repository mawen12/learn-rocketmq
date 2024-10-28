package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import com.mawen.learn.rocketmq.remoting.protocol.body.PopProcessQueueInfo;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@Getter
@Setter
public class PopProcessQueue {

	private static final long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

	private long lastPopTimestamp = System.currentTimeMillis();
	private AtomicInteger waitAckCounter = new AtomicInteger(0);
	private volatile boolean dropped = false;

	public void incFoundMsg(int count) {
		waitAckCounter.getAndAdd(count);
	}

	public void decFoundMsg(int count) {
		waitAckCounter.addAndGet(-count);
	}

	public int ack() {
		return waitAckCounter.getAndIncrement();
	}

	public int getWaitAckMsgCount() {
		return waitAckCounter.get();
	}

	public void fillPopProcessQueueInfo(final PopProcessQueueInfo info) {
		info.setWaitAckCount(getWaitAckMsgCount());
		info.setDropped(isDropped());
		info.setLastPopTimestamp(getLastPopTimestamp());
	}

	public boolean isPullExpired() {
		return (System.currentTimeMillis() - this.lastPopTimestamp) > PULL_MAX_IDLE_TIME;
	}

	@Override
	public String toString() {
		return "PopProcessQueue{" +
				"lastPopTimestamp=" + lastPopTimestamp +
				", waitAckCounter=" + waitAckCounter.get() +
				", dropped=" + dropped +
				'}';
	}
}
