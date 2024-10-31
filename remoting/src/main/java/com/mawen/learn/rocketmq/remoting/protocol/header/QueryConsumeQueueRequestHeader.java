package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/22
 */
@Getter
@Setter
@RocketMQAction(value = RequestCode.QUERY_CONSUME_QUEUE, action = Action.GET)
public class QueryConsumeQueueRequestHeader extends TopicQueueRequestHeader {

	@RocketMQResource(ResourceType.TOPIC)
	private String topic;
	private int queueId;
	private long index;
	private int count;
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@Override
	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	public Integer getQueueId() {
		return queueId;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}
}
