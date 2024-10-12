package com.mawen.learn.rocketmq.remoting.protocol.header.controller;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class AlterSyncStateSetRequestHeader implements CommandCustomHeader {

	private String brokerName;

	private Long masterBrokerId;

	private Integer masterEpoch;

	private long invokeTime = System.currentTimeMillis();

	public AlterSyncStateSetRequestHeader() {
	}

	public AlterSyncStateSetRequestHeader(String brokerName, Long masterBrokerId, Integer masterEpoch) {
		this.brokerName = brokerName;
		this.masterBrokerId = masterBrokerId;
		this.masterEpoch = masterEpoch;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public Long getMasterBrokerId() {
		return masterBrokerId;
	}

	public void setMasterBrokerId(Long masterBrokerId) {
		this.masterBrokerId = masterBrokerId;
	}

	public Integer getMasterEpoch() {
		return masterEpoch;
	}

	public void setMasterEpoch(Integer masterEpoch) {
		this.masterEpoch = masterEpoch;
	}

	public long getInvokeTime() {
		return invokeTime;
	}

	public void setInvokeTime(long invokeTime) {
		this.invokeTime = invokeTime;
	}

	@Override
	public String toString() {
		return "AlterSyncStateSetRequestHeader{" +
				"brokerName='" + brokerName + '\'' +
				", masterBrokerId=" + masterBrokerId +
				", masterEpoch=" + masterEpoch +
				'}';
	}
}
