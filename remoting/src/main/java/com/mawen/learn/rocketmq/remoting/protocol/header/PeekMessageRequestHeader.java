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
@RocketMQAction(value = RequestCode.PEEK_MESSAGE, action = Action.SUB)
public class PeekMessageRequestHeader extends TopicQueueRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private int queueId;

	@CFNotNull
	private int maxMsgNums;

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@Override
	public void checkFields() throws RemotingCommandException {

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
		return queueId;
	}

	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	public int getMaxMsgNums() {
		return maxMsgNums;
	}

	public void setMaxMsgNums(int maxMsgNums) {
		this.maxMsgNums = maxMsgNums;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}
}
