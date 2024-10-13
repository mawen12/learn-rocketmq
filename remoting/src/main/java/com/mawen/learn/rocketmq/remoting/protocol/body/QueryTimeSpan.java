package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Date;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class QueryTimeSpan {

	private MessageQueue messageQueue;

	private long minTimestamp;

	private long maxTimestamp;

	private long consumeTimestamp;

	private long delayTime;

	public MessageQueue getMessageQueue() {
		return messageQueue;
	}

	public void setMessageQueue(MessageQueue messageQueue) {
		this.messageQueue = messageQueue;
	}

	public long getMinTimestamp() {
		return minTimestamp;
	}

	public String getMinTimestampStr() {
		return UtilAll.formatDate(new Date(minTimestamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
	}

	public void setMinTimestamp(long minTimestamp) {
		this.minTimestamp = minTimestamp;
	}

	public long getMaxTimestamp() {
		return maxTimestamp;
	}

	public String getMaxTimestampStr() {
		return UtilAll.formatDate(new Date(maxTimestamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
	}

	public void setMaxTimestamp(long maxTimestamp) {
		this.maxTimestamp = maxTimestamp;
	}

	public long getConsumeTimestamp() {
		return consumeTimestamp;
	}

	public String getConsumeTimestampStr() {
		return UtilAll.formatDate(new Date(consumeTimestamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
	}

	public void setConsumeTimestamp(long consumeTimestamp) {
		this.consumeTimestamp = consumeTimestamp;
	}

	public long getDelayTime() {
		return delayTime;
	}

	public void setDelayTime(long delayTime) {
		this.delayTime = delayTime;
	}
}
