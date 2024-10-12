package com.mawen.learn.rocketmq.remoting.protocol.header.controller.register;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class RegisterBrokerToControllerResponseHeader implements CommandCustomHeader {

	private String clusterName;

	private String brokerName;

	private Long masterBrokerId;

	private String masterAddress;

	private Integer masterEpoch;

	private Integer syncStateSetEpoch;

	public RegisterBrokerToControllerResponseHeader() {
	}

	public RegisterBrokerToControllerResponseHeader(String clusterName, String brokerName) {
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

	public Long getMasterBrokerId() {
		return masterBrokerId;
	}

	public void setMasterBrokerId(Long masterBrokerId) {
		this.masterBrokerId = masterBrokerId;
	}

	public String getMasterAddress() {
		return masterAddress;
	}

	public void setMasterAddress(String masterAddress) {
		this.masterAddress = masterAddress;
	}

	public Integer getMasterEpoch() {
		return masterEpoch;
	}

	public void setMasterEpoch(Integer masterEpoch) {
		this.masterEpoch = masterEpoch;
	}

	public Integer getSyncStateSetEpoch() {
		return syncStateSetEpoch;
	}

	public void setSyncStateSetEpoch(Integer syncStateSetEpoch) {
		this.syncStateSetEpoch = syncStateSetEpoch;
	}
}
