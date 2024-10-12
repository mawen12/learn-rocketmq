package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class BrokerMemberGroup extends RemotingSerializable {

	private String cluster;

	private String brokerName;

	private Map<String, String> brokerAddrs;

	public BrokerMemberGroup() {
		this.brokerAddrs = new HashMap<>();
	}

	public BrokerMemberGroup(String cluster, String brokerName) {
		this.cluster = cluster;
		this.brokerName = brokerName;
		this.brokerAddrs = new HashMap<>();
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public Map<String, String> getBrokerAddrs() {
		return brokerAddrs;
	}

	public void setBrokerAddrs(Map<String, String> brokerAddrs) {
		this.brokerAddrs = brokerAddrs;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BrokerMemberGroup that = (BrokerMemberGroup) o;
		return Objects.equal(cluster, that.cluster) &&
				Objects.equal(brokerName, that.brokerName) &&
				Objects.equal(brokerAddrs, that.brokerAddrs);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(cluster, brokerName, brokerAddrs);
	}

	@Override
	public String toString() {
		return "BrokerMemberGroup{" +
				"cluster='" + cluster + '\'' +
				", brokerName='" + brokerName + '\'' +
				", brokerAddrs=" + brokerAddrs +
				'}';
	}
}
