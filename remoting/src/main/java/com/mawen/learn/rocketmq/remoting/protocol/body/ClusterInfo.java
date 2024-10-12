package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ClusterInfo extends RemotingSerializable {

	private Map<String, BrokerData> brokerAddrTable;

	private Map<String, Set<String>> clusterAddrTable;

	public String[] retrieveAllAddrByCluster(String cluster) {
		List<String> addrs = new ArrayList<>();
		if (clusterAddrTable.containsKey(cluster)) {
			Set<String> brokerNames = clusterAddrTable.get(cluster);
			for (String brokerName : brokerNames) {
				BrokerData brokerData = brokerAddrTable.get(brokerName);
				if (brokerData != null) {
					addrs.addAll(brokerData.getBrokerAddrs().values());
				}
			}
		}
		return addrs.toArray(new String[]{});
	}

	public String[] retrieveAllClusterNames() {
		return clusterAddrTable.keySet().toArray(new String[0]);
	}

	public Map<String, BrokerData> getBrokerAddrTable() {
		return brokerAddrTable;
	}

	public void setBrokerAddrTable(Map<String, BrokerData> brokerAddrTable) {
		this.brokerAddrTable = brokerAddrTable;
	}

	public Map<String, Set<String>> getClusterAddrTable() {
		return clusterAddrTable;
	}

	public void setClusterAddrTable(Map<String, Set<String>> clusterAddrTable) {
		this.clusterAddrTable = clusterAddrTable;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ClusterInfo info = (ClusterInfo) o;
		return Objects.equal(brokerAddrTable, info.brokerAddrTable) && Objects.equal(clusterAddrTable, info.clusterAddrTable);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(brokerAddrTable, clusterAddrTable);
	}
}
