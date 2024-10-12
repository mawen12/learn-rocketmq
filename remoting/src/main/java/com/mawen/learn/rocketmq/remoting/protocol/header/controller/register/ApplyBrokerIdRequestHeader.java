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
@RocketMQAction(value = RequestCode.CONTROLLER_APPLY_BROKER_ID, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class ApplyBrokerIdRequestHeader implements CommandCustomHeader {

	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName;

	private String brokerName;

	private Long appliedBrokerId;

	private String registerCheckCode;

	public ApplyBrokerIdRequestHeader() {
	}

	public ApplyBrokerIdRequestHeader(String clusterName, String brokerName, Long appliedBrokerId, String registerCheckCode) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.appliedBrokerId = appliedBrokerId;
		this.registerCheckCode = registerCheckCode;
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

	public Long getAppliedBrokerId() {
		return appliedBrokerId;
	}

	public void setAppliedBrokerId(Long appliedBrokerId) {
		this.appliedBrokerId = appliedBrokerId;
	}

	public String getRegisterCheckCode() {
		return registerCheckCode;
	}

	public void setRegisterCheckCode(String registerCheckCode) {
		this.registerCheckCode = registerCheckCode;
	}
}
