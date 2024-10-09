package com.mawen.learn.rocketmq.common.statistics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class StatisticsItemScheduledIncrementPrinter extends StatisticsItemScheduledPrinter {

	public static final int TPS_INITIAL_DELAY = 0;
	public static final int TPS_INTERVAL = 1000;
	public static final String SEPARATOR = "|";

	private String[] tpsItemNames;

	private final ConcurrentMap<String, ConcurrentMap<String, StatisticsItem>> lastItemSnapshots = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, ConcurrentMap<String, StatisticsItemSampleBrief>> sampleBriefs = new ConcurrentHashMap<>();

	public StatisticsItemScheduledIncrementPrinter(String name, StatisticsItemPrinter printer, ScheduledExecutorService executor, InitialDelay initialDelay, long interval, String[] tpsItemNames, Valve valve) {
		super(name, printer, executor, initialDelay, interval, valve);
		this.tpsItemNames = tpsItemNames;
	}

	@Override
	public void schedule(StatisticsItem item) {
		setItemSampleBrief(item.getStatKind(), item.getStatObject(), new StatisticsItemSampleBrief(item, tpsItemNames));

		ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
			if (!enabled()) {
				return;
			}

			StatisticsItem snapshot = item.snapshot();
			StatisticsItem lastSnapshot = getItemSnapshot(lastItemSnapshots, item.getStatKind(), item.getStatObject());
			StatisticsItem increment = snapshot.subtract(lastSnapshot);

			Interceptor interceptor = item.getInterceptor();
			String interceptorStr = formatInterceptor(interceptor);
			if (interceptor != null) {
				interceptor.reset();
			}

			StatisticsItemSampleBrief brief = getSampleBrief(item.getStatKind(), item.getStatObject());
			if (brief != null && (!increment.allZeros() || printZeroLine())) {
				printer.print(name, increment, interceptorStr, brief.toString());
			}

			setItemSnapshot(lastItemSnapshots, snapshot);

			if (brief != null) {
				brief.reset();
			}
		}, getInitialDelay(), interval, TimeUnit.MILLISECONDS);
		addFuture(item, future);

		ScheduledFuture<?> futureSample = executor.scheduleAtFixedRate(() -> {
			if (!enabled()) {
				return;
			}

			StatisticsItem snapshot = item.snapshot();
			StatisticsItemSampleBrief brief = getSampleBrief(item.getStatKind(), item.getStatObject());
			if (brief != null) {
				brief.sample(snapshot);
			}
		}, TPS_INTERVAL, TPS_INTERVAL, TimeUnit.MILLISECONDS);
		addFuture(item, futureSample);
	}

	@Override
	public void remove(StatisticsItem item) {
		removeAllFuture(item);

		String kind = item.getStatKind();
		String key = item.getStatObject();

		ConcurrentMap<String, StatisticsItem> lastItemMap = lastItemSnapshots.get(kind);
		if (lastItemMap != null) {
			lastItemMap.remove(key);
		}

		ConcurrentMap<String, StatisticsItemSampleBrief> briefMap = sampleBriefs.get(kind);
		if (briefMap != null) {
			briefMap.remove(key);
		}
	}

	public void setItemSnapshot(ConcurrentMap<String, ConcurrentMap<String, StatisticsItem>> snapshots, StatisticsItem item) {
		String kind = item.getStatKind();
		String key = item.getStatObject();

		ConcurrentMap<String, StatisticsItem> itemMap = snapshots.computeIfAbsent(kind, k -> new ConcurrentHashMap<>());
		itemMap.put(key, item);
	}

	private void setItemSampleBrief(String kind, String key, StatisticsItemSampleBrief brief) {
		ConcurrentMap<String, StatisticsItemSampleBrief> itemMap = sampleBriefs.computeIfAbsent(kind, k -> new ConcurrentHashMap<>());

		itemMap.put(key, brief);
	}

	private String formatInterceptor(Interceptor interceptor) {
		if (interceptor == null) {
			return "";
		}

		if (interceptor instanceof StatisticsBriefInterceptor) {
			StringBuilder sb = new StringBuilder();
			StatisticsBriefInterceptor briefInterceptor = (StatisticsBriefInterceptor) interceptor;
			for (StatisticsBrief brief : briefInterceptor.getStatisticsBriefs()) {
				long max = brief.getMax();
				long tp999 = Math.min(brief.tp999(), max);

				sb.append(SEPARATOR).append(max);
				sb.append(SEPARATOR).append(String.format("%.2f", brief.getAvg()));
				sb.append(SEPARATOR).append(tp999);
			}
			return sb.toString();
		}
		return "";
	}

	private StatisticsItem getItemSnapshot(ConcurrentMap<String, ConcurrentMap<String, StatisticsItem>> snapshots, String kind, String key) {
		ConcurrentMap<String, StatisticsItem> itemMap = snapshots.get(kind);
		return itemMap != null ? itemMap.get(key) : null;
	}

	private StatisticsItemSampleBrief getSampleBrief(String kind, String key) {
		ConcurrentMap<String, StatisticsItemSampleBrief> itemMap = sampleBriefs.get(kind);
		return itemMap != null ? itemMap.get(key) : null;
	}

	public static class StatisticsItemSampleBrief {
		private StatisticsItem lastSnapshot;

		public String[] itemNames;
		public ItemSampleBrief[] briefs;

		public StatisticsItemSampleBrief(StatisticsItem item, String[] itemNames) {
			this.lastSnapshot = item.snapshot();
			this.itemNames = itemNames;
			this.briefs = new ItemSampleBrief[itemNames.length];
			for (int i = 0; i < itemNames.length; i++) {
				this.briefs[i] = new ItemSampleBrief();
			}
		}

		public synchronized void reset() {
			for (ItemSampleBrief brief : briefs) {
				brief.reset();
			}
		}

		public synchronized void sample(StatisticsItem snapshot) {
			if (snapshot == null) {
				return;
			}

			for (int i = 0; i < itemNames.length; i++) {
				String name = itemNames[i];

				long lastValue = lastSnapshot != null ? lastSnapshot.getItemAccumulate(name).get() : 0;
				long increment = snapshot.getItemAccumulate(name).get() - lastValue;
				briefs[i].sample(increment);
			}
			lastSnapshot = snapshot;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < briefs.length; i++) {
				ItemSampleBrief brief = briefs[i];
				sb.append(SEPARATOR).append(brief.getMax());
				sb.append(SEPARATOR).append(String.format("%.2f", brief.getAvg()));
			}
			return sb.toString();
		}
	}

	public static class ItemSampleBrief {
		private long max;
		private long min;
		private long total;
		private long cnt;

		public ItemSampleBrief() {
			reset();
		}

		public void sample(long value) {
			max = Math.max(max, value);
			min = Math.min(min, value);
			total += value;
			cnt++;
		}

		public void reset() {
			max = 0;
			min = Long.MAX_VALUE;
			total = 0;
			cnt = 0;
		}

		public long getMax() {
			return max;
		}

		public long getMin() {
			return cnt > 0 ? min : 0;
		}

		public long getTotal() {
			return total;
		}

		public long getCnt() {
			return cnt;
		}

		public double getAvg() {
			return cnt != 0 ? (double) total / cnt : 0;
		}
	}
}
