package com.mawen.learn.rocketmq.remoting.protocol.header.controller.register;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ApplyBrokerIdResponseHeader implements CommandCustomHeader {

	private String clusterName;

	private String brokerName;

	public ApplyBrokerIdResponseHeader() {
	}

	public ApplyBrokerIdResponseHeader(String clusterName, String brokerName) {
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
		return "ApplyBrokerIdResponseHeader{" +
				"clusterName='" + clusterName + '\'' +
				", brokerName='" + brokerName + '\'' +
				'}';
	}
}
