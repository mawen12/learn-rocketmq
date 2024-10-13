package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class SubscriptionGroupWrapper extends RemotingSerializable {

	private ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>(1024);

	private DataVersion dataVersion = new DataVersion();

	public ConcurrentMap<String,SubscriptionGroupConfig> getSubscriptionGroupTable() {
		return subscriptionGroupTable;
	}

	public void setSubscriptionGroupTable(ConcurrentMap<String,SubscriptionGroupConfig> subscriptionGroupTable) {
		this.subscriptionGroupTable = subscriptionGroupTable;
	}

	public DataVersion getDataVersion() {
		return dataVersion;
	}

	public void setDataVersion(DataVersion dataVersion) {
		this.dataVersion = dataVersion;
	}
}
