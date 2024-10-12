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
@RocketMQAction(value = RequestCode.ACK_MESSAGE, action = Action.SUB)
public class AckMessageRequestHeader extends TopicQueueRequestHeader {

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

	@Override
	public String toString() {
		return "AckMessageRequestHeader{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", topic='" + topic + '\'' +
				", queueId=" + queueId +
				", extraInfo='" + extraInfo + '\'' +
				", offset=" + offset +
				'}';
	}
}
