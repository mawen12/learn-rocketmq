package com.mawen.learn.rocketmq.common.stats;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class RTStatsItem extends StatsItem {

	public RTStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger logger) {
		super(statsName, statsKey, scheduledExecutorService, logger);
	}

	@Override
	protected String statPrintDetail(StatsSnapshot ss) {
		return String.format("TIMES: %d AVGRT: %.2f", ss.getTimes(), ss.getAvgpt());
	}
}
