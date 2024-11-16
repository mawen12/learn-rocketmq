package com.mawen.learn.rocketmq.store.stats;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.MessageStore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
@Getter
@Setter
@RequiredArgsConstructor
public class BrokerStats {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

	private final MessageStore defaultMessageStore;

	private volatile long msgPutTotalYesterdayMorning;

	private volatile long msgPutTotalTodayMorning;

	private volatile long msgGetTotalYesterdayMorning;

	private volatile long msgGetTotalTodayMorning;

	public void record() {
		msgPutTotalYesterdayMorning = msgPutTotalTodayMorning;
		msgGetTotalYesterdayMorning = msgGetTotalTodayMorning;

		msgPutTotalTodayMorning = defaultMessageStore.getBrokerStatsManager().getBrokerPutNumsWithoutSystemTopic();
		msgGetTotalTodayMorning = defaultMessageStore.getBrokerStatsManager().getBrokerGetNumsWithoutSystemTopic();

		log.info("yesterday put message total: {}", msgPutTotalTodayMorning - msgPutTotalYesterdayMorning);
		log.info("yesterday get message total: {}", msgGetTotalTodayMorning - msgGetTotalYesterdayMorning);
	}

	public long getMsgPutTotalTodayNow() {
		return defaultMessageStore.getBrokerStatsManager().getBrokerPutNumsWithoutSystemTopic();
	}

	public long getMsgGetTotalTodayNow() {
		return defaultMessageStore.getBrokerStatsManager().getBrokerGetNumsWithoutSystemTopic();
	}
}
