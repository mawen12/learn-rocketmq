package com.mawen.learn.rocketmq.remoting.protocol.header.controller.admin;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CLEAN_BROKER_DATA, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class CleanControllerBrokerDataRequestHeader implements CommandCustomHeader {

	@CFNullable
	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName;

	@CFNotNull
	private String brokerName;

	@CFNullable
	private String brokerControllerIdsToClean;

	private boolean isCleanLivingBroker = false;

	private long invokeTime = System.currentTimeMillis();

	public CleanControllerBrokerDataRequestHeader() {
	}

	public CleanControllerBrokerDataRequestHeader(String clusterName, String brokerName, String brokerControllerIdsToClean) {
		this(clusterName, brokerName, brokerControllerIdsToClean, false);
	}

	public CleanControllerBrokerDataRequestHeader(String clusterName, String brokerName, String brokerControllerIdsToClean, boolean isCleanLivingBroker) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.brokerControllerIdsToClean = brokerControllerIdsToClean;
		this.isCleanLivingBroker = isCleanLivingBroker;
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

	public String getBrokerControllerIdsToClean() {
		return brokerControllerIdsToClean;
	}

	public void setBrokerControllerIdsToClean(String brokerControllerIdsToClean) {
		this.brokerControllerIdsToClean = brokerControllerIdsToClean;
	}

	public boolean isCleanLivingBroker() {
		return isCleanLivingBroker;
	}

	public void setCleanLivingBroker(boolean cleanLivingBroker) {
		isCleanLivingBroker = cleanLivingBroker;
	}

	public long getInvokeTime() {
		return invokeTime;
	}

	public void setInvokeTime(long invokeTime) {
		this.invokeTime = invokeTime;
	}

	@Override
	public String toString() {
		return "CleanControllerBrokerDataRequestHeader{" +
				"clusterName='" + clusterName + '\'' +
				", brokerName='" + brokerName + '\'' +
				", brokerControllerIdsToClean='" + brokerControllerIdsToClean + '\'' +
				", isCleanLivingBroker=" + isCleanLivingBroker +
				", invokeTime=" + invokeTime +
				'}';
	}
}
