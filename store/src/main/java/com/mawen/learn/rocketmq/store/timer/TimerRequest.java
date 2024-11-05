package com.mawen.learn.rocketmq.store.timer;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.mawen.learn.rocketmq.common.message.MessageExt;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
@Setter
@ToString
public class TimerRequest {
	private final long offsetPy;
	private final int sizePy;
	private final long delayTime;

	private final int magic;

	private long enqueueTime;
	private MessageExt msg;

	private CountDownLatch latch;

	private boolean released;

	private boolean succ;

	private Set<String> deleteList;

	public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic) {
		this(offsetPy, sizePy, delayTime, enqueueTime, magic, null);
	}

	public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic, MessageExt msg) {
		this.offsetPy = offsetPy;
		this.sizePy = sizePy;
		this.delayTime = delayTime;
		this.magic = magic;
		this.enqueueTime = enqueueTime;
		this.msg = msg;
	}

	public void idempotentRelease() {
		idempotentRelease(true);
	}

	public void idempotentRelease(boolean succ) {
		this.succ = succ;
		if (!released && latch != null) {
			released = true;
			latch.countDown();
		}
	}
}
