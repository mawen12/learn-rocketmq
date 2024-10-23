package com.mawen.learn.rocketmq.common.stats;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.mawen.learn.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class StatsItemSet {

	private final ConcurrentMap<String, StatsItem> statsItemTable = new ConcurrentHashMap<>(128);

	private final String statsName;

	private final ScheduledExecutorService scheduledExecutorService;

	private final Logger logger;

	public StatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger logger) {
		this.statsName = statsName;
		this.scheduledExecutorService = scheduledExecutorService;
		this.logger = logger;
		this.init();
	}

	public void init() {
		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				samplingInSeconds();
			}
			catch (Throwable ignored) {
			}
		}, 0, 10, TimeUnit.SECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				samplingInMinutes();
			}
			catch (Throwable ignored) {
			}
		}, 0, 10, TimeUnit.MINUTES);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				samplingInHour();
			}
			catch (Throwable ignored) {
			}
		}, 0, 1, TimeUnit.HOURS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				printAtMinutes();
			}
			catch (Throwable ignored) {
			}
		}, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 60 * 1000, TimeUnit.MILLISECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				printAtHour();
			}
			catch (Throwable ignored) {
			}
		}, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 60 * 60 * 1000, TimeUnit.MILLISECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				printAtDay();
			}
			catch (Throwable ignored) {
			}
		}, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()), 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
	}

	public void addValue(final String statsKey, final int incValue, final int incTimes) {
		StatsItem statsItem = this.getAndCreateStatsItem(statsKey);
		statsItem.getValue().add(incValue);
		statsItem.getTimes().add(incTimes);
	}

	public void addRTValue(final String statsKey, final int incValue, final int incTimes) {
		StatsItem statsItem = this.getAndCreateRTStatsItem(statsKey);
		statsItem.getValue().add(incValue);
		statsItem.getTimes().add(incTimes);
	}

	public void delValue(final String statsKey) {
		StatsItem statsItem = this.statsItemTable.get(statsKey);
		if (statsItem != null) {
			this.statsItemTable.remove(statsKey);
		}
	}

	public void delValueByPrefixKey(final String statsKey, String separator) {
		delValueByPredicateKey(statsKey, next -> next.getKey().startsWith(statsKey + separator));
	}

	public void delValueByInfixKey(final String statsKey, String separator) {
		delValueByPredicateKey(statsKey, next -> next.getKey().contains(separator + statsKey + separator));
	}

	public void delValueBySuffixKey(final String statsKey, String separator) {
		delValueByPredicateKey(statsKey, next -> next.getKey().endsWith(separator + statsKey));
	}

	private void delValueByPredicateKey(final String statsKey, Predicate<Map.Entry<String, StatsItem>> predicateKey) {
		Iterator<Map.Entry<String, StatsItem>> iterator = this.statsItemTable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, StatsItem> next = iterator.next();
			if (predicateKey.test(next)) {
				iterator.remove();
			}
		}
	}

	public StatsItem getAndCreateStatsItem(final String statsKey) {
		return getAndCreateItem(statsKey, false);
	}

	public StatsItem getAndCreateRTStatsItem(final String statsKey) {
		return getAndCreateItem(statsKey, true);
	}

	public StatsItem getAndCreateItem(final String statsKey, boolean rtItem) {
		StatsItem statsItem = this.statsItemTable.get(statsKey);
		if (statsItem == null) {
			if (rtItem) {
				statsItem = new RTStatsItem(this.statsName, statsKey, this.scheduledExecutorService, logger);
			}
			else {
				statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, logger);
			}
			StatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

			if (prev != null) {
				statsItem = prev;
			}
		}

		return statsItem;
	}

	public StatsSnapshot getStatsDataInMinute(final String statsKey) {
		StatsItem statsItem = this.statsItemTable.get(statsKey);
		if (statsItem != null) {
			return statsItem.getStatsDataInMinute();
		}
		return new StatsSnapshot();
	}

	public StatsSnapshot getStatsDataInHour(final String statsKey) {
		StatsItem statsItem = this.statsItemTable.get(statsKey);
		if (statsItem != null) {
			return statsItem.getStatsDataInHour();
		}
		return new StatsSnapshot();
	}

	public StatsSnapshot getStatsDataInDay(final String statsKey) {
		StatsItem statsItem = this.statsItemTable.get(statsKey);
		if (statsItem != null) {
			return statsItem.getStatsDataInDay();
		}
		return new StatsSnapshot();
	}

	public StatsItem getStatsItem(final String statsKey) {
		return this.statsItemTable.get(statsKey);
	}

	private void samplingInSeconds() {
		this.statsItemTable.values().forEach(StatsItem::samplingInSeconds);
	}

	private void samplingInMinutes() {
		this.statsItemTable.values().forEach(StatsItem::samplingInMinutes);
	}

	private void samplingInHour() {
		this.statsItemTable.values().forEach(StatsItem::samplingInHour);
	}

	private void printAtMinutes() {
		this.statsItemTable.values().forEach(StatsItem::printAtMinutes);
	}

	private void printAtHour() {
		this.statsItemTable.values().forEach(StatsItem::printAtHour);
	}

	private void printAtDay() {
		this.statsItemTable.values().forEach(StatsItem::printAtDay);
	}
}
