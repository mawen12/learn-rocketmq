package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.rpc.TopicRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class UpdateGroupForbiddenRequestHeader extends TopicRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String group;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	private Boolean readable;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Boolean getReadable() {
		return readable;
	}

	public void setReadable(Boolean readable) {
		this.readable = readable;
	}
}
