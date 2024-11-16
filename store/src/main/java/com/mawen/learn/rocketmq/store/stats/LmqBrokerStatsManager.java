package com.mawen.learn.rocketmq.store.stats;

import com.mawen.learn.rocketmq.common.MixAll;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public class LmqBrokerStatsManager extends BrokerStatsManager {

	public LmqBrokerStatsManager(String clusterName, boolean enableQueueStat) {
		super(clusterName, enableQueueStat);
	}

	@Override
	public void incGroupGetNums(String group, String topic, int incValue) {
		String lmqGroup = group;
		String lmqTopic = topic;
		if (MixAll.isLmq(group)) {
			lmqGroup = MixAll.LMQ_PREFIX;
		}
		if (MixAll.isLmq(topic)) {
			lmqTopic = MixAll.LMQ_PREFIX;
		}
		super.incGroupGetNums(lmqGroup, lmqTopic, incValue);
	}

	@Override
	public void incGroupGetSize(String group, String topic, int incValue) {
		String lmqGroup = group;
		String lmqTopic = topic;
		if (MixAll.isLmq(group)) {
			lmqGroup = MixAll.LMQ_PREFIX;
		}
		if (MixAll.isLmq(topic)) {
			lmqTopic = MixAll.LMQ_PREFIX;
		}
		super.incGroupGetSize(lmqGroup, lmqTopic, incValue);
	}

	@Override
	public void incGroupGetLatency(String group, String topic, int queueId, int incValue) {
		String lmqGroup = group;
		String lmqTopic = topic;
		if (MixAll.isLmq(group)) {
			lmqGroup = MixAll.LMQ_PREFIX;
		}
		if (MixAll.isLmq(topic)) {
			lmqTopic = MixAll.LMQ_PREFIX;
		}
		super.incGroupGetLatency(lmqGroup, lmqTopic, queueId, incValue);
	}

	@Override
	public void incSendBackNums(String group, String topic) {
		String lmqGroup = group;
		String lmqTopic = topic;
		if (MixAll.isLmq(group)) {
			lmqGroup = MixAll.LMQ_PREFIX;
		}
		if (MixAll.isLmq(topic)) {
			lmqTopic = MixAll.LMQ_PREFIX;
		}
		super.incSendBackNums(lmqGroup, lmqTopic);
	}

	@Override
	public double tpsGroupGetNums(String group, String topic) {
		String lmqGroup = group;
		String lmqTopic = topic;
		if (MixAll.isLmq(group)) {
			lmqGroup = MixAll.LMQ_PREFIX;
		}
		if (MixAll.isLmq(topic)) {
			lmqTopic = MixAll.LMQ_PREFIX;
		}
		return super.tpsGroupGetNums(lmqGroup, lmqTopic);
	}

	@Override
	public void recordDiskFallBehindTime(String group, String topic, int queueId, long fallBehind) {
		String lmqGroup = group;
		String lmqTopic = topic;
		if (MixAll.isLmq(group)) {
			lmqGroup = MixAll.LMQ_PREFIX;
		}
		if (MixAll.isLmq(topic)) {
			lmqTopic = MixAll.LMQ_PREFIX;
		}
		super.recordDiskFallBehindTime(lmqGroup, lmqTopic, queueId, fallBehind);
	}

	@Override
	public void recordDiskFallBehindSize(String group, String topic, int queueId, long fallBehind) {
		String lmqGroup = group;
		String lmqTopic = topic;
		if (MixAll.isLmq(group)) {
			lmqGroup = MixAll.LMQ_PREFIX;
		}
		if (MixAll.isLmq(topic)) {
			lmqTopic = MixAll.LMQ_PREFIX;
		}
		super.recordDiskFallBehindSize(lmqGroup, lmqTopic, queueId, fallBehind);
	}
}
