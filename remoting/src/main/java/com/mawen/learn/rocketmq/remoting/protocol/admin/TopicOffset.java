package com.mawen.learn.rocketmq.remoting.protocol.admin;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class TopicOffset {

	private long minOffset;

	private long maxOffset;

	private long lastUpdateTimestamp;

	public long getMinOffset() {
		return minOffset;
	}

	public void setMinOffset(long minOffset) {
		this.minOffset = minOffset;
	}

	public long getMaxOffset() {
		return maxOffset;
	}

	public void setMaxOffset(long maxOffset) {
		this.maxOffset = maxOffset;
	}

	public long getLastUpdateTimestamp() {
		return lastUpdateTimestamp;
	}

	public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
		this.lastUpdateTimestamp = lastUpdateTimestamp;
	}

	@Override
	public String toString() {
		return "TopicOffset{" +
				"minOffset=" + minOffset +
				", maxOffset=" + maxOffset +
				", lastUpdateTimestamp=" + lastUpdateTimestamp +
				'}';
	}
}
