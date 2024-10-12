package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, action = Action.GET)
public class GetConsumerStatusRequestHeader extends TopicRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String group;

	@CFNullable
	private String clientAddr;

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

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getClientAddr() {
		return clientAddr;
	}

	public void setClientAddr(String clientAddr) {
		this.clientAddr = clientAddr;
	}

	@Override
	public String toString() {
		return "GetConsumerStatusRequestHeader{" +
				"topic='" + topic + '\'' +
				", group='" + group + '\'' +
				", clientAddr='" + clientAddr + '\'' +
				"} " + super.toString();
	}
}
