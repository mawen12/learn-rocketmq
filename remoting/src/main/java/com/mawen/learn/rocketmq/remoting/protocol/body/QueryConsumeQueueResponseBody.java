package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class QueryConsumeQueueResponseBody extends RemotingSerializable {

	private SubscriptionData subscriptionData;

	private String filterData;

	private List<ConsumeQueueData> queueData;

	private long maxQueueIndex;

	private long minQueueIndex;

	public SubscriptionData getSubscriptionData() {
		return subscriptionData;
	}

	public void setSubscriptionData(SubscriptionData subscriptionData) {
		this.subscriptionData = subscriptionData;
	}

	public String getFilterData() {
		return filterData;
	}

	public void setFilterData(String filterData) {
		this.filterData = filterData;
	}

	public List<ConsumeQueueData> getQueueData() {
		return queueData;
	}

	public void setQueueData(List<ConsumeQueueData> queueData) {
		this.queueData = queueData;
	}

	public long getMaxQueueIndex() {
		return maxQueueIndex;
	}

	public void setMaxQueueIndex(long maxQueueIndex) {
		this.maxQueueIndex = maxQueueIndex;
	}

	public long getMinQueueIndex() {
		return minQueueIndex;
	}

	public void setMinQueueIndex(long minQueueIndex) {
		this.minQueueIndex = minQueueIndex;
	}
}
