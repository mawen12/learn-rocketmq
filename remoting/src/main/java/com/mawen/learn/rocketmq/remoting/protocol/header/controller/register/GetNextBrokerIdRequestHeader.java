package com.mawen.learn.rocketmq.remoting.protocol.header.controller.register;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CONTROLLER_GET_NEXT_BROKER_ID, resource = ResourceType.CLUSTER, action = Action.GET)
public class GetNextBrokerIdRequestHeader implements CommandCustomHeader {

	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName;

	private String brokerName;

	public GetNextBrokerIdRequestHeader() {
	}

	public GetNextBrokerIdRequestHeader(String clusterName, String brokerName) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	@Override
	public String toString() {
		return "GetNextBrokerIdRequestHeader{" +
				"clusterName='" + clusterName + '\'' +
				", brokerName='" + brokerName + '\'' +
				'}';
	}
}
