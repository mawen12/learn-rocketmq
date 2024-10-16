package com.mawen.learn.rocketmq.remoting.protocol.body;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class QueryAssignmentRequestBody extends RemotingSerializable {

	private String topic;

	private String consumerGroup;

	private String clientId;

	private String strategyName;

	private MessageModel messageModel;

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

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getStrategyName() {
		return strategyName;
	}

	public void setStrategyName(String strategyName) {
		this.strategyName = strategyName;
	}

	public MessageModel getMessageModel() {
		return messageModel;
	}

	public void setMessageModel(MessageModel messageModel) {
		this.messageModel = messageModel;
	}
}
