package com.mawen.learn.rocketmq.remoting.protocol.body;

import com.mawen.learn.rocketmq.common.message.MessageRequestMode;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class SetMessageRequestModeRequestBody extends RemotingSerializable {

	private String topic;

	private String consumerGroup;

	private MessageRequestMode mode = MessageRequestMode.PULL;

	private int popShareQueueNum = 0;

	public SetMessageRequestModeRequestBody() {
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public MessageRequestMode getMode() {
		return mode;
	}

	public void setMode(MessageRequestMode mode) {
		this.mode = mode;
	}

	public int getPopShareQueueNum() {
		return popShareQueueNum;
	}

	public void setPopShareQueueNum(int popShareQueueNum) {
		this.popShareQueueNum = popShareQueueNum;
	}
}
