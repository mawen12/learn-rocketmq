package com.mawen.learn.rocketmq.remoting.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.consumer.ConsumerFromWhere;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ConsumerData {

	private String groupName;

	private ConsumeType consumeType;

	private MessageModel messageModel;

	private ConsumeFromWhere consumeFromWhere;

	private Set<SubscriptionData> subscriptionDataSet = new HashSet<>();

	private boolean unitMode;

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public ConsumeType getConsumeType() {
		return consumeType;
	}

	public void setConsumeType(ConsumeType consumeType) {
		this.consumeType = consumeType;
	}

	public MessageModel getMessageModel() {
		return messageModel;
	}

	public void setMessageModel(MessageModel messageModel) {
		this.messageModel = messageModel;
	}

	public ConsumeFromWhere getConsumeFromWhere() {
		return consumeFromWhere;
	}

	public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
		this.consumeFromWhere = consumeFromWhere;
	}

	public Set<SubscriptionData> getSubscriptionDataSet() {
		return subscriptionDataSet;
	}

	public void setSubscriptionDataSet(Set<SubscriptionData> subscriptionDataSet) {
		this.subscriptionDataSet = subscriptionDataSet;
	}

	public boolean isUnitMode() {
		return unitMode;
	}

	public void setUnitMode(boolean unitMode) {
		this.unitMode = unitMode;
	}

	@Override
	public String toString() {
		return "ConsumerData{" +
		       "groupName='" + groupName + '\'' +
		       ", consumeType=" + consumeType +
		       ", messageModel=" + messageModel +
		       ", consumeFromWhere=" + consumeFromWhere +
		       ", subscriptionDataSet=" + subscriptionDataSet +
		       ", unitMode=" + unitMode +
		       '}';
	}
}
