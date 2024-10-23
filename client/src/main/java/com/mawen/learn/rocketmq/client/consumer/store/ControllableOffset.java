package com.mawen.learn.rocketmq.client.consumer.store;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public class ControllableOffset {

	private final AtomicLong value;

	private volatile boolean allowToUpdate;

	public ControllableOffset(long value) {
		this.value = new AtomicLong(value);
		this.allowToUpdate = true;
	}

	public void update(long target, boolean increaseOnly) {
		if (allowToUpdate) {
			value.getAndUpdate(val -> allowToUpdate ? (increaseOnly ? Math.max(target, val) : target) : val);
		}
	}

	public void update(long target) {
		update(target, false);
	}

	public void updateAndFreeze(long target) {
		value.getAndUpdate(val -> {
			allowToUpdate = false;
			return target;
		});
	}

	public long getOffset() {
		return value.get();
	}
}
