package com.mawen.learn.rocketmq.common.stats;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.mawen.learn.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class StatsItem {

	private final LongAdder value = new LongAdder();

	private final LongAdder times = new LongAdder();

	private final LinkedList<CallSnapshot> csListMinute = new LinkedList<>();

	private final LinkedList<CallSnapshot> csListHour = new LinkedList<>();

	private final LinkedList<CallSnapshot> csListDay = new LinkedList<>();

	private final String statsName;
	private final String statsKey;
	private final ScheduledExecutorService scheduledExecutorService;

	private final Logger logger;

	public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger logger) {
		this.statsName = statsName;
		this.statsKey = statsKey;
		this.scheduledExecutorService = scheduledExecutorService;
		this.logger = logger;
	}

	public void init() {
		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				samplingInSeconds();
			}
			catch (Exception ignored) {

			}
		}, 0, 10, TimeUnit.SECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				samplingInMinutes();
			}
			catch (Exception ignored) {
			}
		}, 0, 10, TimeUnit.MINUTES);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				samplingInHour();
			}
			catch (Exception ignored) {
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
		}, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
	}

	public void samplingInSeconds() {
		synchronized (this.csListMinute) {
			if (this.csListMinute.size() == 0) {
				this.csListMinute.add(new CallSnapshot(System.currentTimeMillis() - 10 * 1000, 0, 0));
			}
			this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value.sum()));
			if (this.csListMinute.size() > 7) {
				this.csListMinute.removeFirst();
			}
		}
	}

	public void samplingInMinutes() {
		synchronized (this.csListHour) {
			if (this.csListHour.size() == 0) {
				this.csListHour.add(new CallSnapshot(System.currentTimeMillis() - 10 * 60 * 1000, 0, 0));
			}
			this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value.sum()));
			if (this.csListHour.size() > 7) {
				this.csListHour.removeFirst();
			}
		}
	}

	public void samplingInHour() {
		synchronized (this.csListDay) {
			if (this.csListDay.size() == 0) {
				this.csListDay.add(new CallSnapshot(System.currentTimeMillis() - 1 * 60 * 60 * 1000, 0, 0));
			}
			this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value.sum()));
			if (this.csListDay.size() > 25) {
				this.csListDay.removeFirst();
			}
		}
	}

	public void printAtMinutes() {
		StatsSnapshot ss = getStatsDataInMinute();
		logger.info("[{}] [{}] Stats In One Minute, {}", this.statsName, this.statsKey, statPrintDetail(ss));
	}

	public void printAtHour() {
		StatsSnapshot ss = getStatsDataInHour();
		logger.info("[{}] [{}] Stats In One Hour, {}", this.statsName, this.statsKey, statPrintDetail(ss));
	}

	public void printAtDay() {
		StatsSnapshot ss = getStatsDataInDay();
		logger.info("[{}] [{}] Stats In One Day, {}", this.statsName, this.statsKey, statPrintDetail(ss));
	}

	protected String statPrintDetail(StatsSnapshot ss) {
		return String.format("SUM: %d TPS: %.2f AVGPT: %.2f",
				ss.getSum(), ss.getTps(), ss.getAvgpt());
	}

	public StatsSnapshot getStatsDataInMinute() {
		return computeStatsData(this.csListMinute);
	}

	public StatsSnapshot getStatsDataInHour() {
		return computeStatsData(this.csListHour);
	}

	public StatsSnapshot getStatsDataInDay() {
		return computeStatsData(this.csListDay);
	}

	private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
		StatsSnapshot statsSnapshot = new StatsSnapshot();

		synchronized (csList) {
			double tps = 0;
			double avgpt = 0;
			long sum = 0;
			long timesDiff = 0;

			if (!csList.isEmpty()) {
				CallSnapshot first = csList.getFirst();
				CallSnapshot last = csList.getLast();

				sum = last.getValue() - first.getValue();
				tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());
				timesDiff = last.getTimes() - first.getTimes();
				if (timesDiff > 0) {
					avgpt = (sum * 1.0d) / timesDiff;
				}
			}

			statsSnapshot.setSum(sum);
			statsSnapshot.setTps(tps);
			statsSnapshot.setAvgpt(avgpt);
			statsSnapshot.setTimes(timesDiff);
		}

		return statsSnapshot;
	}

	public LongAdder getValue() {
		return value;
	}

	public LongAdder getTimes() {
		return times;
	}

	public String getStatsName() {
		return statsName;
	}

	public String getStatsKey() {
		return statsKey;
	}

	class CallSnapshot {
		private final long timestamp;
		private final long times;
		private final long value;

		public CallSnapshot(long timestamp, long times, long value) {
			this.timestamp = timestamp;
			this.times = times;
			this.value = value;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getTimes() {
			return times;
		}

		public long getValue() {
			return value;
		}
	}
}
