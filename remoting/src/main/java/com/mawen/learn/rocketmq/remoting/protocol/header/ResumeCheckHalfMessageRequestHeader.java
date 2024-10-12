package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.RESUME_CHECK_HALF_MESSAGE, action = Action.UPDATE)
public class ResumeCheckHalfMessageRequestHeader implements CommandCustomHeader {

	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNullable
	private String msgId;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	@Override
	public String toString() {
		return "ResumeCheckHalfMessageRequestHeader{" +
				"msgId='" + msgId + '\'' +
				'}';
	}
}
