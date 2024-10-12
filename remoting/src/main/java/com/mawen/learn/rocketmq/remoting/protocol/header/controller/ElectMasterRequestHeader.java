package com.mawen.learn.rocketmq.remoting.protocol.header.controller;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CONTROLLER_ELECT_MASTER, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class ElectMasterRequestHeader implements CommandCustomHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName = "";

	@CFNotNull
	private String brokerName = "";

	@CFNotNull
	private Long brokerId = -1L;

	@CFNotNull
	private Boolean designateElect = false;

	private Long invokeTime = System.currentTimeMillis();

	public ElectMasterRequestHeader() {
	}

	public ElectMasterRequestHeader(String brokerName) {
		this.brokerName = brokerName;
	}

	public ElectMasterRequestHeader(String clusterName, String brokerName, Long brokerId) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.brokerId = brokerId;
	}

	public ElectMasterRequestHeader(String clusterName, String brokerName, Long brokerId, Boolean designateElect) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.brokerId = brokerId;
		this.designateElect = designateElect;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public static ElectMasterRequestHeader ofBrokerTrigger(String clusterName, String brokerName, Long brokerId) {
		return new ElectMasterRequestHeader(clusterName, brokerName, brokerId);
	}

	public static ElectMasterRequestHeader ofControllerTrigger(String brokerName) {
		return new ElectMasterRequestHeader(brokerName);
	}

	public static ElectMasterRequestHeader ofAdminTrigger(String clusterName, String brokerName, Long brokerId) {
		return new ElectMasterRequestHeader(clusterName, brokerName, brokerId, true);
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

	public Boolean getDesignateElect() {
		return designateElect;
	}

	public void setDesignateElect(Boolean designateElect) {
		this.designateElect = designateElect;
	}

	public Long getInvokeTime() {
		return invokeTime;
	}

	public void setInvokeTime(Long invokeTime) {
		this.invokeTime = invokeTime;
	}

	@Override
	public String toString() {
		return "ElectMasterRequestHeader{" +
				"clusterName='" + clusterName + '\'' +
				", brokerName='" + brokerName + '\'' +
				", brokerId=" + brokerId +
				", designateElect=" + designateElect +
				'}';
	}
}
