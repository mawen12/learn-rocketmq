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
@RocketMQAction(value = RequestCode.CONTROLLER_REGISTER_BROKER, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class RegisterBrokerToControllerRequestHeader implements CommandCustomHeader {

	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName;

	private String brokerName;

	private Long brokerId;

	private String brokerAddress;

	private long invokeTime;

	public RegisterBrokerToControllerRequestHeader() {
	}

	public RegisterBrokerToControllerRequestHeader(String clusterName, String brokerName, Long brokerId, String brokerAddress) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.brokerId = brokerId;
		this.brokerAddress = brokerAddress;
		this.invokeTime = System.currentTimeMillis();
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

	public Long getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(Long brokerId) {
		this.brokerId = brokerId;
	}

	public String getBrokerAddress() {
		return brokerAddress;
	}

	public void setBrokerAddress(String brokerAddress) {
		this.brokerAddress = brokerAddress;
	}

	public long getInvokeTime() {
		return invokeTime;
	}

	public void setInvokeTime(long invokeTime) {
		this.invokeTime = invokeTime;
	}
}
