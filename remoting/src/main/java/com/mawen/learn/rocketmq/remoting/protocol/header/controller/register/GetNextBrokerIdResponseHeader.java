package com.mawen.learn.rocketmq.remoting.protocol.header.controller.register;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class GetNextBrokerIdResponseHeader implements CommandCustomHeader {

	private String clusterName;

	private String brokerName;

	private Long nextBrokerId;

	public GetNextBrokerIdResponseHeader() {
	}

	public GetNextBrokerIdResponseHeader(String clusterName, String brokerName) {
		this(clusterName, brokerName, null);
	}

	public GetNextBrokerIdResponseHeader(String clusterName, String brokerName, Long nextBrokerId) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.nextBrokerId = nextBrokerId;
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

	public Long getNextBrokerId() {
		return nextBrokerId;
	}

	public void setNextBrokerId(Long nextBrokerId) {
		this.nextBrokerId = nextBrokerId;
	}

	@Override
	public String toString() {
		return "GetNextBrokerIdResponseHeader{" +
				"clusterName='" + clusterName + '\'' +
				", brokerName='" + brokerName + '\'' +
				", nextBrokerId=" + nextBrokerId +
				'}';
	}
}
