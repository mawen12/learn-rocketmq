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
public class MomentStatsItemSet {

	private final ConcurrentMap<String, MomentStatsItem> statsItemTable = new ConcurrentHashMap<>(128);

	private final String statsName;

	private final ScheduledExecutorService scheduledExecutorService;

	private final Logger log;

	public MomentStatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger log) {
		this.statsName = statsName;
		this.scheduledExecutorService = scheduledExecutorService;
		this.log = log;
	}

	public void init() {
		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				printAtMinutes();
			}
			catch (Throwable ignored) {
			}
		}, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 5 * 60 * 1000, TimeUnit.MILLISECONDS);
	}

	public void setValue(final String statsKey, final int value) {
		MomentStatsItem statsItem = this.getAndCreateStatsItem(statsKey);
		statsItem.getValue().set(value);
	}

	public void delValueByPrefixKey(final String statsKey, String separator) {
		delValueByPredicateKey(statsKey, next -> next.getKey().endsWith(separator + statsKey));
	}

	public void delValueByInfixKey(final String statsKey, String separator) {
		delValueByPredicateKey(statsKey, next -> next.getKey().contains(separator + statsKey + separator));
	}

	public void delValueBySuffixKey(final String statsKey, String separator) {
		delValueByPredicateKey(statsKey, next -> next.getKey().endsWith(separator + statsKey));
	}

	private void delValueByPredicateKey(final String statsKey, Predicate<Map.Entry<String, MomentStatsItem>> predicateKey) {
		Iterator<Map.Entry<String, MomentStatsItem>> iterator = this.statsItemTable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, MomentStatsItem> next = iterator.next();
			if (predicateKey.test(next)) {
				iterator.remove();
			}
		}
	}

	public MomentStatsItem getAndCreateStatsItem(final String statsKey) {
		MomentStatsItem statsItem = this.statsItemTable.get(statsKey);

		if (statsItem == null) {
			statsItem = new MomentStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
			MomentStatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

			if (prev != null) {
				statsItem = prev;
			}
		}
		return statsItem;
	}

	private void printAtMinutes() {
		this.statsItemTable.values().forEach(MomentStatsItem::printAtMinutes);
	}

	public ConcurrentMap<String, MomentStatsItem> getStatsItemTable() {
		return statsItemTable;
	}

	public String getStatsName() {
		return statsName;
	}
}
