package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class StatisticsMessagesRequestHeader extends TopicQueueRequestHeader {

	@CFNotNull
	private String consumerGroup;

	@CFNotNull
	private String topic;

	@CFNotNull
	private int queueId;

	private long fromTime;

	private long toTime;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public Integer getQueueId() {
		if (queueId < 0) {
			return -1;
		}
		return queueId;
	}

	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	public long getFromTime() {
		return fromTime;
	}

	public void setFromTime(long fromTime) {
		this.fromTime = fromTime;
	}

	public long getToTime() {
		return toTime;
	}

	public void setToTime(long toTime) {
		this.toTime = toTime;
	}
}
