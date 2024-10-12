package com.mawen.learn.rocketmq.remoting.protocol.admin;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class OffsetWrapper {

	private long brokerOffset;

	private long consumerOffset;

	private long pullOffset;

	private long lastTimestamp;

	public long getBrokerOffset() {
		return brokerOffset;
	}

	public void setBrokerOffset(long brokerOffset) {
		this.brokerOffset = brokerOffset;
	}

	public long getConsumerOffset() {
		return consumerOffset;
	}

	public void setConsumerOffset(long consumerOffset) {
		this.consumerOffset = consumerOffset;
	}

	public long getPullOffset() {
		return pullOffset;
	}

	public void setPullOffset(long pullOffset) {
		this.pullOffset = pullOffset;
	}

	public long getLastTimestamp() {
		return lastTimestamp;
	}

	public void setLastTimestamp(long lastTimestamp) {
		this.lastTimestamp = lastTimestamp;
	}
}
