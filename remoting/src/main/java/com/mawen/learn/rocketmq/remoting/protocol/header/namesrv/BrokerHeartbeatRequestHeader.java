package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import org.checkerframework.checker.units.qual.C;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.BROKER_HEARTBEAT, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class BrokerHeartbeatRequestHeader implements CommandCustomHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName;

	@CFNotNull
	private String brokerAddr;

	@CFNotNull
	private String brokerName;

	@CFNotNull
	private Long brokerId;

	@CFNotNull
	private Integer epoch;

	@CFNotNull
	private Long maxOffset;

	@CFNotNull
	private Long confirmOffset;

	@CFNotNull
	private Long heartbeatTimeoutMillis;

	@CFNotNull
	private Integer electionPriority;


	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getBrokerAddr() {
		return brokerAddr;
	}

	public void setBrokerAddr(String brokerAddr) {
		this.brokerAddr = brokerAddr;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public Long getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(Long brokerId) {
		this.brokerId = brokerId;
	}

	public Integer getEpoch() {
		return epoch;
	}

	public void setEpoch(Integer epoch) {
		this.epoch = epoch;
	}

	public Long getMaxOffset() {
		return maxOffset;
	}

	public void setMaxOffset(Long maxOffset) {
		this.maxOffset = maxOffset;
	}

	public Long getConfirmOffset() {
		return confirmOffset;
	}

	public void setConfirmOffset(Long confirmOffset) {
		this.confirmOffset = confirmOffset;
	}

	public Long getHeartbeatTimeoutMillis() {
		return heartbeatTimeoutMillis;
	}

	public void setHeartbeatTimeoutMillis(Long heartbeatTimeoutMillis) {
		this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
	}

	public Integer getElectionPriority() {
		return electionPriority;
	}

	public void setElectionPriority(Integer electionPriority) {
		this.electionPriority = electionPriority;
	}
}
