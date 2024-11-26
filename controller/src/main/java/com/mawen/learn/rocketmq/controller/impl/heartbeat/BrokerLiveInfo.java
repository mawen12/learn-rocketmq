package com.mawen.learn.rocketmq.controller.impl.heartbeat;

import java.io.Serializable;
import java.nio.channels.Channel;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@Getter
@Setter
@ToString
public class BrokerLiveInfo implements Serializable {

	private static final long serialVersionUID = 2151003774595535025L;

	private final String brokerName;

	private String brokerAddr;
	private long heartbeatTimeoutMillis;
	private Channel channel;
	private long brokerId;
	private long lastUpdateTimestamp;
	private int epoch;
	private long maxOffset;
	private long confirmOffset;
	private Integer electionPriority;

	public BrokerLiveInfo(String brokerName, String brokerAddr, long brokerId, long lastUpdateTimestamp, long heartbeatTimeoutMillis, Channel channel, int epoch, long maxOffset, Integer electionPriority) {
		this.brokerName = brokerName;
		this.brokerAddr = brokerAddr;
		this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
		this.channel = channel;
		this.brokerId = brokerId;
		this.lastUpdateTimestamp = lastUpdateTimestamp;
		this.epoch = epoch;
		this.maxOffset = maxOffset;
		this.electionPriority = electionPriority;
	}

	public BrokerLiveInfo(String brokerName, String brokerAddr, long brokerId, long lastUpdateTimestamp, long heartbeatTimeoutMillis, Channel channel, int epoch, long maxOffset, Integer electionPriority, long confirmOffset) {
		this.brokerName = brokerName;
		this.brokerAddr = brokerAddr;
		this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
		this.channel = channel;
		this.brokerId = brokerId;
		this.lastUpdateTimestamp = lastUpdateTimestamp;
		this.epoch = epoch;
		this.maxOffset = maxOffset;
		this.confirmOffset = confirmOffset;
		this.electionPriority = electionPriority;
	}
}
