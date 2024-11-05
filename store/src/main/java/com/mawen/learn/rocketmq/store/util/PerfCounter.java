package com.mawen.learn.rocketmq.store.util;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public class PerfCounter {

	private long last = System.currentTimeMillis();
	private float lastTps = 0.0f;

	private final ThreadLocal<AtomicLong> lastTickMs = new ThreadLocal<AtomicLong>() {
		@Override
		protected AtomicLong initialValue() {
			return new AtomicLong(System.currentTimeMillis());
		}
	};

	private final Logger log;

	private String prefix = "DEFAULT";


	private final AtomicInteger[] count;
	private final AtomicLong allCount;
	private final int maxNumPerCount;
	private final int maxTimeMsPerCount;

	public PerfCounter() {
		this(5001, null, null, 1000 * 1000, 10 * 1000);
	}

	public PerfCounter(int slots, Logger log, String prefix, int maxNumPerCount, int maxTimeMsPerCount) {
		if (slots < 3000) {
			throw new RuntimeException("slots must bigger than 3000, but:" + slots);
		}

		count = new AtomicInteger[slots];
		allCount = new AtomicLong(0);
		this.log = log;
		if (prefix != null) {
			this.prefix = prefix;
		}
		this.maxNumPerCount = maxNumPerCount;
		this.maxTimeMsPerCount = maxTimeMsPerCount;

		reset();
	}

	public void flow(long cost) {
		flow(cost, 1);
	}

	public void flow(long cost, int num) {
		if (cost < 0) {
			return;
		}

		allCount.addAndGet(num);
		count[getIndex(cost)].addAndGet(num);
		if (allCount.get() >= maxTimeMsPerCount || System.currentTimeMillis() - last >= maxTimeMsPerCount) {
			synchronized (allCount) {
				if (allCount.get() < maxNumPerCount && System.currentTimeMillis() - last < maxNumPerCount) {
					return;
				}
				print();
				reset();
			}
		}
	}

	public void print() {
		if (log == null) {
			return;
		}

		int min = getMin();
		int max = getMax();
		int tp50 = getTPValue(0.5f);
		int tp80 = getTPValue(0.8f);
		int tp90 = getTPValue(0.9f);
		int tp99 = getTPValue(0.99f);
		int tp999 = getTPValue(0.999f);

		long count0t1 = getCount(0, 1);
		long count2t5 = getCount(2, 5);
		long count6t10 = getCount(6, 10);
		long count11t50 = getCount(11, 50);
		long count51t100 = getCount(51, 100);
		long count101t500 = getCount(101, 500);
		long count501t999 = getCount(501, 999);
		long count1000t = getCount(1000, 100_000_000);

		long elapsed = System.currentTimeMillis() - last;
		lastTps = (allCount.get() + 0.1f) * 1000 / elapsed;

		String str = String.format("PERF_COUNTER_%s[%s] num:%d cost:%d tps:%.4f min:%d max:%d tp50:%d tp80:%d tp90:%d tp99:%d tp999:%d" +
						"0_1:%d 2_5:%d 6_10:%d 11_50:%d 51_100:%d 101_500:%d 501_999:%d 1000_:%d",
				prefix, new Timestamp(System.currentTimeMillis()), allCount.get(), elapsed, lastTps, min, max, tp50, tp80, tp90, tp99, tp999,
				count0t1, count2t5, count6t10, count11t50, count51t100, count101t500, count501t999, count1000t);

		log.info(str);
	}

	public long getCount(int from, int to) {
		from = getIndex(from);
		to = getIndex(to);
		long tmp = 0;
		for (int i = from; i <= to && i < count.length; i++) {
			tmp += count[i].get();
		}
		return tmp;
	}

	private int getIndex(long cost) {
		if (cost < 1000) {
			return (int) cost;
		}

		if (cost >= 1000 && cost < 1000 + 10 * 1000) {
			int units = (int) ((cost - 1000) / 10);
			return units + 1000;
		}

		int units = (int) ((cost - 1000 - 10 * 1000) / 100);
		units += 2000;

		if (units >= count.length) {
			units = count.length - 1;
		}
		return units;
	}

	private int convert(int index) {
		if (index < 1000) {
			return index;
		}
		else if (index >= 1000 && index < 2000) {
			return (index - 1000) * 10 + 1000;
		}
		else {
			return (index - 2000) * 100 + 1000 * 10 + 1000;
		}
	}

	public int getTPValue(float ratio) {
		if (ratio <= 0 || ratio >= 1) {
			ratio = 0.99f;
		}

		long num = (long) (allCount.get() * (1 - ratio));
		int tmp = 0;
		for (int i = count.length - 1; i >= 0; i--) {
			tmp += count[i].get();
			if (tmp > num) {
				return convert(i);
			}
		}
		return 0;
	}

	public int getMin() {
		for (int i = 0; i < count.length; i++) {
			if (count[i].get() > 0) {
				return convert(i);
			}
		}
		return 0;
	}

	public int getMax() {
		for (int i = count.length - 1; i >= 0; i--) {
			if (count[i].get() > 0) {
				return convert(i);
			}
		}
		return 99999999;
	}

	public void reset() {
		for (int i = 0; i < count.length; i++) {
			if (count[i] == null) {
				count[i] = new AtomicInteger(0);
			}
			else {
				count[i].set(0);
			}
		}
		allCount.set(0);
		last = System.currentTimeMillis();
	}

	public void startTick() {
		lastTickMs.get().set(System.currentTimeMillis());
	}

	public void endTick() {
		flow(System.currentTimeMillis() - lastTickMs.get().get());
	}

	public static class Ticks extends ServiceThread {

		private final Logger log;
		private final ConcurrentMap<String, PerfCounter> perfs = new ConcurrentHashMap<>();
		private final ConcurrentMap<String, AtomicLong> keyFreqs = new ConcurrentHashMap<>();
		private final PerfCounter defaultPerf;
		private final AtomicLong defaultTime = new AtomicLong(System.currentTimeMillis());

		private final int maxKeyNumPerf;
		private final int maxKeyNumDebug;

		private final int maxNumPerCount;
		private final int maxTimeMsPerCount;

		public Ticks() {
			this(null, 1000 * 1000, 10 * 1000, 20 * 1000, 100 * 1000);
		}

		public Ticks(Logger log) {
			this(log, 1000 * 1000, 10 * 1000, 20 * 1000, 100 * 1000);
		}

		public Ticks(Logger log, int maxKeyNumPerf, int maxKeyNumDebug, int maxNumPerCount, int maxTimeMsPerCount) {
			this.log = log;
			this.maxKeyNumPerf = maxKeyNumPerf;
			this.maxKeyNumDebug = maxKeyNumDebug;
			this.maxNumPerCount = maxNumPerCount;
			this.maxTimeMsPerCount = maxTimeMsPerCount;
			this.defaultPerf = new PerfCounter(3001, log, null, maxNumPerCount, maxTimeMsPerCount);
		}

		@Override
		public String getServiceName() {
			return this.getClass().getName();
		}

		@Override
		public void run() {
			log.info("{} get started", getServiceName());

			while (!isStopped()) {
				try {
					long maxLiveTimeMs = maxTimeMsPerCount * 2 + 1000;
					waitForRunning(maxLiveTimeMs);
					if (perfs.size() >= maxKeyNumPerf || keyFreqs.size() >= maxKeyNumDebug) {
						log.warn("The key is full {}-{}-{}-{}", perfs.size(), maxKeyNumPerf, keyFreqs.size(), maxKeyNumDebug);
					}

					{
						Iterator<Map.Entry<String, PerfCounter>> iterator = perfs.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, PerfCounter> entry = iterator.next();
							PerfCounter value = entry.getValue();
							if (System.currentTimeMillis() - value.last > maxLiveTimeMs) {
								iterator.remove();
							}
						}
					}

					{
						Iterator<Map.Entry<String, AtomicLong>> iterator = keyFreqs.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, AtomicLong> entry = iterator.next();
							AtomicLong value = entry.getValue();
							if (System.currentTimeMillis() - value.get() > maxLiveTimeMs) {
								iterator.remove();
							}
						}
					}
				}
				catch (Exception e) {
					log.error("{} get unknown error", getServiceName(), e);
					try {
						Thread.sleep(1000);
					}
					catch (Throwable ignored) {}
				}
			}

			log.info("{} get stopped", getServiceName());
		}

		public void startTick(String key) {
			try {
				makeSureExists(key).startTick();
			}
			catch (Throwable ignored) {}
		}

		public void endTick(String key) {
			try {
				makeSureExists(key).endTick();
			}
			catch (Throwable ignored) {}
		}

		public void flowOnce(String key, int cost) {
			try {
				makeSureExists(key).flow(cost);
			}
			catch (Throwable ignored) {}
		}

		public PerfCounter getCounter(String key) {
			try {
				return makeSureExists(key);
			}
			catch (Throwable ignored) {
				return defaultPerf;
			}
		}

		private PerfCounter makeSureExists(String key) {
			if (perfs.get(key) == null) {
				if (perfs.size() >= maxKeyNumPerf + 100) {
					return defaultPerf;
				}
				perfs.put(key, new PerfCounter(3001, log, key, maxKeyNumPerf, maxTimeMsPerCount));
			}
			return perfs.getOrDefault(key, defaultPerf);
		}

		private AtomicLong makeSureDebugKeyExists(String key) {
			AtomicLong lastTimeMs = keyFreqs.get(key);
			if (lastTimeMs == null) {
				if (keyFreqs.size() >= maxKeyNumDebug + 100) {
					return defaultTime;
				}
				lastTimeMs = new AtomicLong(0);
				keyFreqs.put(key, lastTimeMs);
			}
			return keyFreqs.getOrDefault(key, defaultTime);
		}
	}
}
