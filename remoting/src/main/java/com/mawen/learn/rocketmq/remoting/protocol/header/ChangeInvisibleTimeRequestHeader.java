package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CHANGE_MESSAGE_INVISIBLETIME, action = Action.SUB)
public class ChangeInvisibleTimeRequestHeader implements CommandCustomHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private Integer queueId;

	@CFNotNull
	private String extraInfo;

	@CFNotNull
	private Long offset;

	@CFNotNull
	private Long invisibleTime;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Integer getQueueId() {
		return queueId;
	}

	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	public String getExtraInfo() {
		return extraInfo;
	}

	public void setExtraInfo(String extraInfo) {
		this.extraInfo = extraInfo;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Long getInvisibleTime() {
		return invisibleTime;
	}

	public void setInvisibleTime(Long invisibleTime) {
		this.invisibleTime = invisibleTime;
	}

	@Override
	public String toString() {
		return "ChangeInvisibleTimeRequestHeader{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", topic='" + topic + '\'' +
				", queueId=" + queueId +
				", extraInfo='" + extraInfo + '\'' +
				", offset=" + offset +
				", invisibleTime=" + invisibleTime +
				'}';
	}
}
