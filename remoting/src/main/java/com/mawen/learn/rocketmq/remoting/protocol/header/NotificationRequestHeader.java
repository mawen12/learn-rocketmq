package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.NOTIFICATION, action = Action.SUB)
public class NotificationRequestHeader extends TopicQueueRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private int queueId;

	@CFNotNull
	private long pollTime;

	@CFNotNull
	private long bornTime;

	private Boolean order = Boolean.FALSE;

	private String attemptId;

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

	public long getPollTime() {
		return pollTime;
	}

	public void setPollTime(long pollTime) {
		this.pollTime = pollTime;
	}

	public long getBornTime() {
		return bornTime;
	}

	public void setBornTime(long bornTime) {
		this.bornTime = bornTime;
	}

	public Boolean getOrder() {
		return order;
	}

	public void setOrder(Boolean order) {
		this.order = order;
	}

	public String getAttemptId() {
		return attemptId;
	}

	public void setAttemptId(String attemptId) {
		this.attemptId = attemptId;
	}

	@Override
	public String toString() {
		return "NotificationRequestHeader{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", topic='" + topic + '\'' +
				", queueId=" + queueId +
				", pollTime=" + pollTime +
				", bornTime=" + bornTime +
				", order=" + order +
				", attemptId='" + attemptId + '\'' +
				"} " + super.toString();
	}
}
