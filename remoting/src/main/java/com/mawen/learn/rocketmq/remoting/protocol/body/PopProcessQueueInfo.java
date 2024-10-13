package com.mawen.learn.rocketmq.remoting.protocol.body;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class PopProcessQueueInfo {

	private int waitAckCount;

	private boolean dropped;

	private long lastPopTimestamp;

	public int getWaitAckCount() {
		return waitAckCount;
	}

	public void setWaitAckCount(int waitAckCount) {
		this.waitAckCount = waitAckCount;
	}

	public boolean isDropped() {
		return dropped;
	}

	public void setDropped(boolean dropped) {
		this.dropped = dropped;
	}

	public long getLastPopTimestamp() {
		return lastPopTimestamp;
	}

	public void setLastPopTimestamp(long lastPopTimestamp) {
		this.lastPopTimestamp = lastPopTimestamp;
	}

	@Override
	public String toString() {
		return "PopProcessQueueInfo{" +
		       "waitAckCount=" + waitAckCount +
		       ", dropped=" + dropped +
		       ", lastPopTimestamp=" + lastPopTimestamp +
		       '}';
	}
}
