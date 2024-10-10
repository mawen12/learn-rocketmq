package com.mawen.learn.rocketmq.common.stats;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class StatsSnapshot {

	private long sum;
	private double tps;

	private long times;
	private double avgpt;

	public long getSum() {
		return sum;
	}

	public void setSum(long sum) {
		this.sum = sum;
	}

	public double getTps() {
		return tps;
	}

	public void setTps(double tps) {
		this.tps = tps;
	}

	public long getTimes() {
		return times;
	}

	public void setTimes(long times) {
		this.times = times;
	}

	public double getAvgpt() {
		return avgpt;
	}

	public void setAvgpt(double avgpt) {
		this.avgpt = avgpt;
	}
}
