package com.mawen.learn.rocketmq.remoting.protocol.subscription;

import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class ExponentialRetryPolicy implements RetryPolicy{

	private long initial = TimeUnit.SECONDS.toMillis(5);

	private long max = TimeUnit.HOURS.toMillis(2);

	private long multiplier = 2;

	public ExponentialRetryPolicy() {
	}

	public ExponentialRetryPolicy(long initial, long max, long multiplier) {
		this.initial = initial;
		this.max = max;
		this.multiplier = multiplier;
	}

	@Override
	public long nextDelayDuration(int reconsumeTimes) {
		if (reconsumeTimes < 0) {
			reconsumeTimes = 0;
		}

		if (reconsumeTimes > 32) {
			reconsumeTimes = 32;
		}

		return Math.min(max, initial * (long) Math.pow(multiplier, reconsumeTimes));
	}

	public long getInitial() {
		return initial;
	}

	public void setInitial(long initial) {
		this.initial = initial;
	}

	public long getMax() {
		return max;
	}

	public void setMax(long max) {
		this.max = max;
	}

	public long getMultiplier() {
		return multiplier;
	}

	public void setMultiplier(long multiplier) {
		this.multiplier = multiplier;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("initial", initial)
				.add("max", max)
				.add("multiplier", multiplier)
				.toString();
	}
}
