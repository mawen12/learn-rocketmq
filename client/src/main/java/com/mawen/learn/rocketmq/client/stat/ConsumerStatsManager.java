package com.mawen.learn.rocketmq.client.stat;

import java.util.concurrent.ScheduledExecutorService;

import com.mawen.learn.rocketmq.common.stats.StatsItemSet;
import com.mawen.learn.rocketmq.common.stats.StatsSnapshot;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeStatus;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public class ConsumerStatsManager {
	private static final Logger log = LoggerFactory.getLogger(ConsumerStatsManager.class);

	private static final String TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
	private static final String TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_FAILED_TPS";
	private static final String TOPIC_AND_GROUP_CONSUME_RT = "CONSUME_RT";
	private static final String TOPIC_AND_GROUP_PULL_TPS = "PULL_TPS";
	private static final String TOPIC_AND_GROUP_PULL_RT = "PULL_RT";

	private final StatsItemSet topicAndGroupConsumeOKTPS;
	private final StatsItemSet topicAndGroupConsumeRT;
	private final StatsItemSet topicAndGroupConsumeFailedTPS;
	private final StatsItemSet topicAndGroupPullTPS;
	private final StatsItemSet topicAndGroupPullRT;

	public ConsumerStatsManager(ScheduledExecutorService scheduledExecutorService) {
		this.topicAndGroupConsumeOKTPS = new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_TPS, scheduledExecutorService, log);
		this.topicAndGroupConsumeRT = new StatsItemSet(TOPIC_AND_GROUP_CONSUME_RT, scheduledExecutorService, log);
		this.topicAndGroupConsumeFailedTPS = new StatsItemSet(TOPIC_AND_GROUP_CONSUME_FAILED_TPS, scheduledExecutorService, log);
		this.topicAndGroupPullRT = new StatsItemSet(TOPIC_AND_GROUP_PULL_RT, scheduledExecutorService, log);
		this.topicAndGroupPullTPS = new StatsItemSet(TOPIC_AND_GROUP_PULL_TPS, scheduledExecutorService, log);
	}

	public void start() {

	}

	public void shutdown() {

	}

	public void incPullRT(final String group, final String topic, final long rt) {
		this.topicAndGroupPullRT.addRTValue(topic + "@" + group, (int) rt, 1);
	}

	public void incPullTPS(final String group, final String topic, final long msgs) {
		this.topicAndGroupPullTPS.addRTValue(topic + "@" + group, (int) msgs, 1);
	}

	public void incConsumeRT(final String group, final String topic, final long rt) {
		this.topicAndGroupConsumeRT.addRTValue(topic + "@" + group, (int) rt, 1);
	}

	public void incConsumeOKTPS(final String group, final String topic, final long msgs) {
		this.topicAndGroupConsumeOKTPS.addRTValue(topic + "@" + group, (int) msgs, 1);
	}

	public void incConsumeFailedTPS(final String group, final String topic, final long msgs) {
		this.topicAndGroupConsumeFailedTPS.addRTValue(topic + "@" + group, (int) msgs, 1);
	}

	public StatsSnapshot getPullRT(final String group, final String topic) {
		return this.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group);
	}

	public StatsSnapshot getPullTPS(final String group, final String topic) {
		return this.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group);
	}

	public StatsSnapshot getConsumeRT(final String group, final String topic) {
		StatsSnapshot statsData = this.topicAndGroupConsumeRT.getStatsDataInMinute(topic + "@" + group);
		if (statsData.getSum() == 0) {
			statsData = this.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group);
		}
		return statsData;
	}

	public StatsSnapshot getConsumeOKTPS(final String group, final String topic) {
		return this.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group);
	}

	public StatsSnapshot getConsumeFailedTPS(final String group, final String topic) {
		return this.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group);
	}

	public ConsumeStatus consumeStatus(final String group, final String topic) {
		ConsumeStatus cs = new ConsumeStatus();
		StatsSnapshot ss = this.getPullRT(group, topic);
		if (ss != null) {
			cs.setPullRT(ss.getAvgpt());
		}

		ss = this.getPullTPS(group, topic);
		if (ss != null) {
			cs.setPullTPS(ss.getTps());
		}

		ss = this.getConsumeRT(group, topic);
		if (ss != null) {
			cs.setConsumeRT(ss.getAvgpt());
		}

		ss = this.getConsumeOKTPS(group, topic);
		if (ss != null) {
			cs.setConsumeOKTPS(ss.getTps());
		}

		ss = this.getConsumeFailedTPS(group, topic);
		if (ss != null) {
			cs.setConsumeFailedTPS(ss.getTps());
		}

		ss = this.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group);
		if (ss != null) {
			cs.setConsumeFailedMsgs(ss.getSum());
		}

		return cs;
	}
}
