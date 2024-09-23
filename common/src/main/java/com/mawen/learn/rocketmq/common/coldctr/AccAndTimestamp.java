package com.mawen.learn.rocketmq.common.coldctr;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class AccAndTimestamp {

	private AtomicLong coldAcc = new AtomicLong(0L);
	private Long lastColdReadTimeMills = System.currentTimeMillis();
	private Long createTimeMills = System.currentTimeMillis();

	public AccAndTimestamp(AtomicLong coldAcc) {
		this.coldAcc = coldAcc;
	}

	public AtomicLong getColdAcc() {
		return coldAcc;
	}

	public void setColdAcc(AtomicLong coldAcc) {
		this.coldAcc = coldAcc;
	}

	public Long getLastColdReadTimeMills() {
		return lastColdReadTimeMills;
	}

	public void setLastColdReadTimeMills(Long lastColdReadTimeMills) {
		this.lastColdReadTimeMills = lastColdReadTimeMills;
	}

	public Long getCreateTimeMills() {
		return createTimeMills;
	}

	public void setCreateTimeMills(Long createTimeMills) {
		this.createTimeMills = createTimeMills;
	}

	@Override
	public String toString() {
		return "AccAndTimestamp{" +
		       "coldAcc=" + coldAcc +
		       ", lastColdReadTimeMills=" + lastColdReadTimeMills +
		       ", createTimeMills=" + createTimeMills +
		       '}';
	}
}
