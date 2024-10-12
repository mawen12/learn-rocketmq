package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class NotifyMinBrokerIdChangeRequestHeader implements CommandCustomHeader {

	@CFNullable
	private Long minBrokerId;

	@CFNullable
	private String brokerName;

	@CFNullable
	private String minBrokerAddr;

	@CFNullable
	private String offlineBrokerAddr;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Long getMinBrokerId() {
		return minBrokerId;
	}

	public void setMinBrokerId(Long minBrokerId) {
		this.minBrokerId = minBrokerId;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public String getMinBrokerAddr() {
		return minBrokerAddr;
	}

	public void setMinBrokerAddr(String minBrokerAddr) {
		this.minBrokerAddr = minBrokerAddr;
	}

	public String getOfflineBrokerAddr() {
		return offlineBrokerAddr;
	}

	public void setOfflineBrokerAddr(String offlineBrokerAddr) {
		this.offlineBrokerAddr = offlineBrokerAddr;
	}
}
