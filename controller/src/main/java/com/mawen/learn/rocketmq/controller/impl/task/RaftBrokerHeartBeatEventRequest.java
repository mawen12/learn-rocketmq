package com.mawen.learn.rocketmq.controller.impl.task;

import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
@ToString
@NoArgsConstructor
public class RaftBrokerHeartBeatEventRequest implements CommandCustomHeader {

	private String clusterNameIdentifyInfo;

	private String brokerNameIdentifyInfo;

	private Long brokerIdIdentifyInfo;

	private String brokerName;
	private String brokerAddr;
	private Long heartbeatTimeoutMillis;
	private Long brokerId;
	private Long lastUpdateTimestamp;
	private Integer epoch;
	private Long maxOffset;
	private Long confirmOffset;
	private Integer electionPriority;

	public RaftBrokerHeartBeatEventRequest(BrokerIdentityInfo info, BrokerLiveInfo brokerLiveInfo) {
		this.clusterNameIdentifyInfo = info.getClusterName();
		this.brokerNameIdentifyInfo = info.getBrokerName();
		this.brokerIdIdentifyInfo = info.getBrokerId();

		this.brokerName = brokerLiveInfo.getBrokerName();
		this.brokerAddr = brokerLiveInfo.getBrokerAddr();
		this.heartbeatTimeoutMillis = brokerLiveInfo.getHeartbeatTimeoutMillis();
		this.brokerId = brokerLiveInfo.getBrokerId();
		this.lastUpdateTimestamp = brokerLiveInfo.getLastUpdateTimestamp();
		this.epoch = brokerLiveInfo.getEpoch();
		this.maxOffset = brokerLiveInfo.getMaxOffset();
		this.confirmOffset = brokerLiveInfo.getConfirmOffset();
		this.electionPriority = brokerLiveInfo.getElectionPriority();
	}

	public BrokerIdentityInfo getBrokerIdentifyInfo() {
		return new BrokerIdentityInfo(clusterNameIdentifyInfo, brokerNameIdentifyInfo, brokerIdIdentifyInfo);
	}

	public void setBrokerIdentifyInfo(BrokerIdentityInfo info) {
		this.clusterNameIdentifyInfo = info.getClusterName();
		this.brokerNameIdentifyInfo = info.getBrokerName();
		this.brokerIdIdentifyInfo = info.getBrokerId();
	}

	public BrokerLiveInfo getBrokerLiveInfo() {
		return new BrokerLiveInfo(brokerName, brokerAddr, brokerId, lastUpdateTimestamp, heartbeatTimeoutMillis, null, epoch, maxOffset, electionPriority, confirmOffset);
	}

	public void setBrokerLiveInfo(BrokerLiveInfo brokerLiveInfo) {
		this.brokerName = brokerLiveInfo.getBrokerName();
		this.brokerAddr = brokerLiveInfo.getBrokerAddr();
		this.heartbeatTimeoutMillis = brokerLiveInfo.getHeartbeatTimeoutMillis();
		this.brokerId = brokerLiveInfo.getBrokerId();
		this.lastUpdateTimestamp = brokerLiveInfo.getLastUpdateTimestamp();
		this.epoch = brokerLiveInfo.getEpoch();
		this.maxOffset = brokerLiveInfo.getMaxOffset();
		this.confirmOffset = brokerLiveInfo.getConfirmOffset();
		this.electionPriority = brokerLiveInfo.getElectionPriority();
	}

	@Override
	public void checkFields() throws RemotingCommandException {
		// NOP
	}
}
