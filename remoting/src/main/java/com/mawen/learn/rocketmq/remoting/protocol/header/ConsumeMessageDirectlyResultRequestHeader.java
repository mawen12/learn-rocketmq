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
@RocketMQAction(value = RequestCode.CONSUME_MESSAGE_DIRECTLY, action = Action.SUB)
public class ConsumeMessageDirectlyResultRequestHeader extends TopicRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@CFNullable
	private String clientId;

	@CFNullable
	private String msgId;

	@CFNullable
	private String brokerName;

	@CFNullable
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNullable
	private Integer topicSysFlag;

	@CFNullable
	private Integer groupSysFlag;

	@Override
	public void checkFields() throws RemotingCommandException {

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

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	@Override
	public String getBrokerName() {
		return brokerName;
	}

	@Override
	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Integer getTopicSysFlag() {
		return topicSysFlag;
	}

	public void setTopicSysFlag(Integer topicSysFlag) {
		this.topicSysFlag = topicSysFlag;
	}

	public Integer getGroupSysFlag() {
		return groupSysFlag;
	}

	public void setGroupSysFlag(Integer groupSysFlag) {
		this.groupSysFlag = groupSysFlag;
	}

	@Override
	public String toString() {
		return "ConsumeMessageDirectlyResultRequestHeader{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", clientId='" + clientId + '\'' +
				", msgId='" + msgId + '\'' +
				", brokerName='" + brokerName + '\'' +
				", topic='" + topic + '\'' +
				", topicSysFlag=" + topicSysFlag +
				", groupSysFlag=" + groupSysFlag +
				"} " + super.toString();
	}
}
