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
@RocketMQAction(value = RequestCode.POLLING_INFO, action = Action.GET)
public class PollingInfoRequestHeader extends TopicQueueRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private int queueId;

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
}
