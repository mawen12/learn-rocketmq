package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.EpochEntry;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class EpochEntryCache extends RemotingSerializable {

	private String clusterName;

	private String brokerName;

	private long brokerId;

	private List<EpochEntry> epochList;

	private long maxOffset;

	public EpochEntryCache(String clusterName, String brokerName, long brokerId, List<EpochEntry> epochList, long maxOffset) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.brokerId = brokerId;
		this.epochList = epochList;
		this.maxOffset = maxOffset;
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

	public long getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(long brokerId) {
		this.brokerId = brokerId;
	}

	public List<EpochEntry> getEpochList() {
		return epochList;
	}

	public void setEpochList(List<EpochEntry> epochList) {
		this.epochList = epochList;
	}

	public long getMaxOffset() {
		return maxOffset;
	}

	public void setMaxOffset(long maxOffset) {
		this.maxOffset = maxOffset;
	}

	@Override
	public String toString() {
		return "EpochEntryCache{" +
		       "clusterName='" + clusterName + '\'' +
		       ", brokerName='" + brokerName + '\'' +
		       ", brokerId=" + brokerId +
		       ", epochList=" + epochList +
		       ", maxOffset=" + maxOffset +
		       "} " + super.toString();
	}
}
