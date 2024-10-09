package com.mawen.learn.rocketmq.common.statistics;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.checkerframework.checker.units.qual.A;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class StatisticsItem {

	private String statKind;
	private String statObject;

	private String[] itemNames;
	private AtomicLong[] itemAccumulates;
	private AtomicLong invokeTimes;

	private Interceptor interceptor;

	private AtomicLong lastTimestamp;

	public StatisticsItem(String statKind, String statObject, String... itemNames) {
		if (itemNames == null || itemNames.length <= 0) {
			throw new InvalidParameterException("StatisticsItem \"itemNames\" is empty");
		}

		this.statKind = statKind;
		this.statObject = statObject;
		this.itemNames = itemNames;

		AtomicLong[] accs = new AtomicLong[itemNames.length];
		for (int i = 0; i < itemNames.length; i++) {
			accs[i] = new AtomicLong(0);
		}

		this.itemAccumulates = accs;
		this.invokeTimes = new AtomicLong();
		this.lastTimestamp = new AtomicLong(System.currentTimeMillis());
	}

	public void incItems(long... itemIncs) {
		int len = Math.min(itemIncs.length, itemAccumulates.length);
		for (int i = 0; i < len; i++) {
			itemAccumulates[i].addAndGet(itemIncs[i]);
		}

		invokeTimes.addAndGet(1);
		lastTimestamp.set(System.currentTimeMillis());

		if (interceptor != null) {
			interceptor.inc(itemIncs);
		}
	}

	public boolean allZeros() {
		if (invokeTimes.get() == 0) {
			return true;
		}

		for (AtomicLong acc : itemAccumulates) {
			if (acc.get() != 0) {
				return false;
			}
		}
		return true;
	}

	public StatisticsItem subtract(StatisticsItem item) {
		if (item == null) {
			return snapshot();
		}

		if (!statKind.equals(item.statKind) || !statObject.equals(item.statObject) || !Arrays.equals(itemNames, item.itemNames)) {
			throw new IllegalArgumentException("Statistics's kind, key and itemNames must be exactly the same");
		}

		StatisticsItem ret = new StatisticsItem(statKind, statObject, itemNames);
		ret.invokeTimes = new AtomicLong(invokeTimes.get() - item.invokeTimes.get());
		ret.itemAccumulates = new AtomicLong[itemAccumulates.length];
		for (int i = 0; i < itemAccumulates.length; i++) {
			ret.itemAccumulates[i] = new AtomicLong(itemAccumulates[i].get() - item.itemAccumulates[i].get());
		}
		return ret;
	}

	public StatisticsItem snapshot() {
		StatisticsItem ret = new StatisticsItem(statKind, statObject, itemNames);

		ret.itemAccumulates = new AtomicLong[itemAccumulates.length];
		for (int i = 0; i < itemAccumulates.length; i++) {
			ret.itemAccumulates[i] = new AtomicLong(itemAccumulates[i].get());
		}

		ret.invokeTimes = new AtomicLong(invokeTimes.longValue());
		ret.lastTimestamp = new AtomicLong(lastTimestamp.longValue());

		return ret;
	}

	public String getStatKind() {
		return statKind;
	}

	public String getStatObject() {
		return statObject;
	}

	public String[] getItemNames() {
		return itemNames;
	}

	public AtomicLong[] getItemAccumulates() {
		return itemAccumulates;
	}

	public AtomicLong getItemAccumulate(String itemName) {
		int index = ArrayUtils.indexOf(itemNames, itemName);
		if (index < 0) {
			return new AtomicLong(0);
		}
		return itemAccumulates[index];
	}

	public AtomicLong getInvokeTimes() {
		return invokeTimes;
	}

	public Interceptor getInterceptor() {
		return interceptor;
	}

	public void setInterceptor(Interceptor interceptor) {
		this.interceptor = interceptor;
	}

	public AtomicLong getLastTimestamp() {
		return lastTimestamp;
	}
}
