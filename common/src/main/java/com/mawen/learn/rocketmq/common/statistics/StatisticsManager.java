package com.mawen.learn.rocketmq.common.statistics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
@Setter
@Getter
public class StatisticsManager {

	private static final int MAX_IDLE_TIME = 10 * 60 * 1000;

	private Map<String, StatisticsKindMeta> kindMetaMap;

	private Pair<String, long[][]>[] briefMetas;

	private StatisticsItemStateGetter statisticsItemStateGetter;

	private final ConcurrentMap<String, ConcurrentMap<String, StatisticsItem>> statsTable = new ConcurrentHashMap<>();

	private final ScheduledExecutorService executor = ThreadUtils.newSingleThreadScheduledExecutor("StatisticsManagerCleaner", true);

	public StatisticsManager() {
		this.kindMetaMap = new HashMap<>();
		start();
	}

	public StatisticsManager(Map<String, StatisticsKindMeta> kindMeta) {
		this.kindMetaMap = kindMeta;
		start();
	}

	public void addStatisticsKindMeta(StatisticsKindMeta kindMeta) {
		kindMetaMap.put(kindMeta.getName(), kindMeta);
		statsTable.putIfAbsent(kindMeta.getName(), new ConcurrentHashMap<>(16));
	}

	public boolean inc(String kind, String key, long... itemAccumulates) {
		ConcurrentMap<String, StatisticsItem> itemMap = statsTable.get(key);
		if (itemMap != null) {
			StatisticsItem item = itemMap.get(key);

			if (item == null) {
				item = new StatisticsItem(kind, key, kindMetaMap.get(kind).getItemNames());
				item.setInterceptor(new StatisticsBriefInterceptor(item, briefMetas));
				StatisticsItem oldItem = itemMap.putIfAbsent(key, item);
				if (oldItem != null) {
					item = oldItem;
				}
				else {
					scheduleStatisticsItem(item);
				}
			}

			item.incItems(itemAccumulates);

			return true;
		}
		return false;
	}

	public void remove(StatisticsItem item) {
		ConcurrentMap<String, StatisticsItem> itemMap = statsTable.get(item.getStatKind());
		if (itemMap != null) {
			itemMap.remove(item.getStatObject(), item);
		}

		StatisticsKindMeta kindMeta = kindMetaMap.get(item.getStatKind());
		if (kindMeta != null && kindMeta.getScheduledPrinter() != null) {
			kindMeta.getScheduledPrinter().remove(item);
		}
	}

	private void start() {
		int maxIdleTime = MAX_IDLE_TIME;
		executor.scheduleAtFixedRate(() -> {
			Iterator<Map.Entry<String, ConcurrentMap<String, StatisticsItem>>> iterator = statsTable.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<String, ConcurrentMap<String, StatisticsItem>> entry = iterator.next();
				String kind = entry.getKey();
				ConcurrentMap<String, StatisticsItem> itemMap = entry.getValue();

				if (itemMap == null || itemMap.isEmpty()) {
					continue;
				}

				Map<String, StatisticsItem> tmpItemMap = new HashMap<>(itemMap);
				for (StatisticsItem item : tmpItemMap.values()) {
					if (System.currentTimeMillis() - item.getLastTimestamp().get() > MAX_IDLE_TIME
							&& (statisticsItemStateGetter == null || !statisticsItemStateGetter.online(item))) {
						remove(item);
					}
				}
			}
		}, maxIdleTime, maxIdleTime / 3, TimeUnit.MILLISECONDS);
	}

	private void scheduleStatisticsItem(StatisticsItem item) {
		kindMetaMap.get(item.getStatKind()).getScheduledPrinter().schedule(item);
	}
}
