package com.mawen.learn.rocketmq.remoting.protocol.body;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class BrokerStatsData extends RemotingSerializable {

	private BrokerStatsItem statsMinute;

	private BrokerStatsItem statsHour;

	private BrokerStatsItem statsDay;

	public BrokerStatsItem getStatsMinute() {
		return statsMinute;
	}

	public void setStatsMinute(BrokerStatsItem statsMinute) {
		this.statsMinute = statsMinute;
	}

	public BrokerStatsItem getStatsHour() {
		return statsHour;
	}

	public void setStatsHour(BrokerStatsItem statsHour) {
		this.statsHour = statsHour;
	}

	public BrokerStatsItem getStatsDay() {
		return statsDay;
	}

	public void setStatsDay(BrokerStatsItem statsDay) {
		this.statsDay = statsDay;
	}
}
