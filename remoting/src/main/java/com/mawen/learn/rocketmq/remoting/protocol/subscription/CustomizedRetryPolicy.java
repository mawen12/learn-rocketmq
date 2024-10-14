package com.mawen.learn.rocketmq.remoting.protocol.subscription;

import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class CustomizedRetryPolicy implements RetryPolicy {

	private long[] next = new long[]{
			TimeUnit.SECONDS.toMillis(1),
			TimeUnit.SECONDS.toMillis(5),
			TimeUnit.SECONDS.toMillis(10),
			TimeUnit.SECONDS.toMillis(30),
			TimeUnit.MINUTES.toMillis(1),
			TimeUnit.MINUTES.toMillis(2),
			TimeUnit.MINUTES.toMillis(3),
			TimeUnit.MINUTES.toMillis(4),
			TimeUnit.MINUTES.toMillis(5),
			TimeUnit.MINUTES.toMillis(6),
			TimeUnit.MINUTES.toMillis(7),
			TimeUnit.MINUTES.toMillis(8),
			TimeUnit.MINUTES.toMillis(9),
			TimeUnit.MINUTES.toMillis(10),
			TimeUnit.MINUTES.toMillis(20),
			TimeUnit.MINUTES.toMillis(30),
			TimeUnit.HOURS.toMillis(1),
			TimeUnit.HOURS.toMillis(2),
	};

	public CustomizedRetryPolicy() {
	}

	public CustomizedRetryPolicy(long[] next) {
		this.next = next;
	}

	@Override
	public long nextDelayDuration(int reconsumeTimes) {
		if (reconsumeTimes < 0) {
			reconsumeTimes = 0;
		}
		int index = reconsumeTimes + 2;
		if (index >= next.length) {
			index = next.length - 1;
		}
		return next[index];
	}

	public long[] getNext() {
		return next;
	}

	public void setNext(long[] next) {
		this.next = next;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("next", next)
				.toString();
	}
}
