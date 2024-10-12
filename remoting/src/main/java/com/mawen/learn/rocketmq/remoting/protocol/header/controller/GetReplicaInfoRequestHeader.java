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
@RocketMQAction(value = RequestCode.CONTROLLER_GET_REPLICA_INFO, resource = ResourceType.CLUSTER, action = Action.GET)
public class GetReplicaInfoRequestHeader implements CommandCustomHeader {

	private String brokerName;

	public GetReplicaInfoRequestHeader() {
	}

	public GetReplicaInfoRequestHeader(String brokerName) {
		this.brokerName = brokerName;
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

	@Override
	public String toString() {
		return "GetReplicaInfoRequestHeader{" +
				"brokerName='" + brokerName + '\'' +
				'}';
	}
}
