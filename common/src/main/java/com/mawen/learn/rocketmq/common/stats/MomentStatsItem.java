package com.mawen.learn.rocketmq.common.stats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class MomentStatsItem {

	private final AtomicLong value = new AtomicLong(0);

	private final String statsName;

	private final String statsKey;

	private final ScheduledExecutorService scheduledExecutorService;

	private final Logger log;

	public MomentStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger log) {
		this.statsName = statsName;
		this.statsKey = statsKey;
		this.scheduledExecutorService = scheduledExecutorService;
		this.log = log;
	}

	public void init() {
		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				printAtMinutes();

				MomentStatsItem.this.value.set(0);
			}
			catch (Throwable ignored) {
			}
		}, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 5 * 60 * 1000, TimeUnit.MILLISECONDS);
	}

	public void printAtMinutes() {
		log.info("[{}] [{}] Stats Every 5 Minutes, Value: {}",
				this.statsName, this.statsKey, this.value.get());
	}

	public AtomicLong getValue() {
		return value;
	}

	public String getStatsName() {
		return statsName;
	}

	public String getStatsKey() {
		return statsKey;
	}
}
