package com.mawen.learn.rocketmq.remoting.protocol.body;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class BrokerStatsItem {

	private long sum;

	private double tps;

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

	public double getAvgpt() {
		return avgpt;
	}

	public void setAvgpt(double avgpt) {
		this.avgpt = avgpt;
	}
}
